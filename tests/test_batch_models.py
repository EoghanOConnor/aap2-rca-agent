"""Batch models and generated-output validation preserve the JSON contracts."""

from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from unittest.mock import AsyncMock

import jsonschema
import pytest
from pydantic import ValidationError

from rca.batch import orchestrator, store_report
from rca.config import Config
from rca.models import BatchReport, BatchSemanticOutput


def _semantics() -> dict:
    return {
        "historical_matches": [
            {
                "job_id": "101",
                "matches": [
                    {
                        "matched_result_id": 9,
                        "similarity_reasoning": "Same worker timeout",
                        "confidence": "high",
                        "evidence": [
                            {"statement_type": "observed", "description": "Worker timed out"}
                        ],
                    }
                ],
            }
        ],
        "cross_job_patterns": [
            {"pattern": "Timeout", "jobs": ["101", "102"], "description": "Same worker"}
        ],
    }


def _report() -> dict:
    semantics = _semantics()
    match = semantics["historical_matches"][0]["matches"][0]
    return {
        "batch_id": "batch_test",
        "generated_at": "2026-10-28T13:00:00Z",
        "total_jobs_requested": 2,
        "total_jobs_analyzed": 1,
        "total_jobs_failed": 1,
        "timing": {
            "agent_spawn": "2026-10-28T12:59:00Z",
            "agent_completion": {"101": {"duration_ms": 100, "status": "completed"}},
            "wall_clock_total_ms": 1000,
            "aggregation_completed_at": "2026-10-28T13:00:00Z",
        },
        "root_cause_category_breakdown": {
            "infrastructure": {"count": 1, "job_ids": ["101"], "description": "Infrastructure"}
        },
        "confidence_breakdown": {"high": 1, "medium": 0, "low": 0},
        "high_priority_recommendations": [
            {
                "rank": 1,
                "priority": "high",
                "action": "Restart worker",
                "affects_jobs": ["101"],
                "category": "infrastructure",
                "details": "Worker timed out",
            }
        ],
        "job_summaries": [
            {
                "job_id": "101",
                "status": "analyzed",
                "historical_matches": [{**match, "recurrence_count": 2}],
            }
        ],
        "failed_analyses": [{"job_id": "102", "error": "Unavailable", "stage": "upload"}],
        "cross_job_patterns": [
            {**semantics["cross_job_patterns"][0], "source": "current_batch"}
        ],
    }


def _validator(model):
    return jsonschema.Draft202012Validator(model.model_json_schema())


def _set(data, path, value):
    for key in path[:-1]:
        data = data[key]
    data[path[-1]] = value


@pytest.mark.parametrize("model,payload", [(BatchReport, _report()), (BatchSemanticOutput, _semantics())])
def test_batch_models_preserve_open_properties_and_omitted_fields(model, payload):
    payload = copy.deepcopy(payload)
    payload["extra_context"] = {"arbitrary": [None, True]}
    payload["cross_job_patterns"][0]["extra"] = "preserved"
    _validator(model).validate(payload)
    assert model.model_validate(payload).model_dump(exclude_unset=True) == payload
    for field in model.model_fields:
        missing = {key: value for key, value in payload.items() if key != field}
        with pytest.raises(ValidationError):
            model.model_validate(missing)


@pytest.mark.parametrize(
    "path,value",
    [
        (("total_jobs_requested",), -1),
        (("total_jobs_analyzed",), True),
        (("total_jobs_failed",), "1"),
        (("timing", "agent_completion", "101", "duration_ms"), -1),
        (("timing", "agent_completion", "101", "status"), "invalid"),
        (("root_cause_category_breakdown", "infrastructure", "count"), 0),
        (("confidence_breakdown", "low"), -1),
        (("high_priority_recommendations", 0, "rank"), 0),
        (("high_priority_recommendations", 0, "rank"), 6),
        (("high_priority_recommendations", 0, "category"), "invalid"),
        (("high_priority_recommendations", 0, "github_path"), None),
        (("job_summaries", 0, "status"), "invalid"),
        (("job_summaries", 0, "confidence"), "invalid"),
        (("job_summaries", 0, "result_id"), "9"),
        (("job_summaries", 0, "job_duration_seconds"), -1),
        (("job_summaries", 0, "historical_matches", 0, "recurrence_count"), 0),
        (("job_summaries", 0, "historical_matches", 0, "confidence"), None),
        (("failed_analyses", 0, "stage"), "invalid"),
        (("cross_job_patterns", 0, "source"), "invalid"),
        (("cross_job_patterns", 0, "jobs", 0), 101),
    ],
)
def test_report_rejects_schema_invalid_nested_values(path, value):
    payload = _report()
    _set(payload, path, value)
    assert not _validator(BatchReport).is_valid(payload)
    with pytest.raises(ValidationError):
        BatchReport.model_validate(payload)


def test_report_limits_priority_recommendations():
    payload = _report()
    payload["high_priority_recommendations"] *= 6
    assert not _validator(BatchReport).is_valid(payload)
    with pytest.raises(ValidationError):
        BatchReport.model_validate(payload)


def test_report_validation_preserves_integral_floats_and_original_dict():
    payload = _report()
    payload["total_jobs_requested"] = 2.0
    original = copy.deepcopy(payload)
    _validator(BatchReport).validate(payload)
    orchestrator._validate_batch_report(payload)
    assert payload == original
    assert type(payload["total_jobs_requested"]) is float
    assert "confidence" not in payload["job_summaries"][0]


@pytest.mark.parametrize("as_text", [False, True])
@pytest.mark.parametrize(
    "output",
    [
        {},
        {"historical_matches": {}, "cross_job_patterns": []},
        {"historical_matches": [{"job_id": 101, "matches": []}], "cross_job_patterns": []},
        {"historical_matches": [], "cross_job_patterns": [{"pattern": "Missing fields"}]},
    ],
)
def test_raw_semantic_output_is_validated_before_normalizing(
    monkeypatch, tmp_path, as_text, output
):
    query = orchestrator.SDKQueryResult(
        False, None, {}, json.dumps(output) if as_text else "", structured_output=None if as_text else output
    )
    monkeypatch.setattr(orchestrator, "_run_sdk_query", AsyncMock(return_value=query))
    summaries = [
        {"job_id": "101", "status": "analyzed", "confidence": "high"},
        {"job_id": "102", "status": "analyzed", "confidence": "high"},
    ]
    original = copy.deepcopy(summaries)
    config = Config.from_env(environment={}, env_file=tmp_path / "missing.env")
    with pytest.raises(ValueError, match="Semantic batch output failed schema validation"):
        asyncio.run(
            orchestrator._aggregate_semantics(
                summaries, [], config, cwd=tmp_path, semaphore=asyncio.Semaphore(1)
            )
        )
    assert summaries == original


def test_invalid_root_cause_retains_warning_behavior(capsys):
    payload = store_report.build_root_cause({"root_cause_summary": None}, "101")
    assert payload["summary"] is None
    assert "assembled root_cause failed schema validation" in capsys.readouterr().err


def test_invalid_nested_report_cannot_be_written(tmp_path: Path):
    payload = _report()
    payload["job_summaries"][0]["result_id"] = True
    config = Config.from_env(
        environment={"RCA_STATE_DIR": str(tmp_path)}, env_file=tmp_path / "missing.env"
    )
    with pytest.raises(ValueError, match=r"job_summaries.0.result_id"):
        orchestrator._write_report(config, payload)
    assert not (tmp_path / "reports").exists()
