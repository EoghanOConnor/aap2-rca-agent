"""Validate typed summary/detail contracts and the summary API schema."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest
from pydantic import ValidationError

from rca.models.summary import AnalysisSummary, RootCauseDetail


def _summary() -> dict[str, Any]:
    return {
        "job_id": "101",
        "root_cause": {
            "summary": "Worker timed out",
            "category": "infrastructure",
            "confidence": "high",
        },
        "correlation": {"method": "namespace_time_match", "identifiers": {}},
        "evidence": [{"source": "aap_job", "timestamp": "", "message": "Timeout"}],
    }


def _detail() -> dict[str, Any]:
    return {
        "summary": "Worker timed out",
        "evidence": [
            {
                "source": "related_job",
                "job_id": "101",
                "timestamp": None,
                "message": "Timeout",
                "github_path": None,
            }
        ],
        "causal_chain": [
            {
                "step": 1,
                "relationship": "direct_cause",
                "statement": "The worker stopped responding",
                "evidence_ref": None,
            }
        ],
        "misidentifications": [
            {
                "theory": "Invalid configuration",
                "why_suspected": "Provisioning failed",
                "why_ruled_out": "Configuration was valid",
                "evidence_ref": None,
            }
        ],
        "recommendations": [
            {
                "priority": "high",
                "action": "Restart worker",
                "github_path": None,
                "details": None,
                "evidence_ref": None,
                "fix": None,
            }
        ],
    }


def _assert_parity(
    model: type[AnalysisSummary] | type[RootCauseDetail],
    data: dict[str, Any],
    *,
    valid: bool,
) -> None:
    validator = jsonschema.Draft202012Validator(
        model.model_json_schema(), format_checker=jsonschema.Draft7Validator.FORMAT_CHECKER
    )
    assert validator.is_valid(data) is valid
    if valid:
        parsed = model.model_validate(data)
        assert parsed.model_dump(exclude_unset=True) == data
    else:
        with pytest.raises(ValidationError):
            model.model_validate(data)


@pytest.mark.parametrize("model,data", [(AnalysisSummary, _summary()), (RootCauseDetail, _detail())])
def test_models_preserve_valid_payloads_and_open_properties(model, data) -> None:
    data = copy.deepcopy(data)
    _assert_parity(model, data, valid=True)
    data["future_context"] = {"anything": [1, None, True]}
    _assert_parity(model, data, valid=True)


@pytest.mark.parametrize(
    "fields",
    [
        {"recommendations": [{}]},
        {"recommendations": [{"evidence_ref": None, "fix": None}]},
        {"recommendations": [{"evidence_ref": 0.0}]},
        {"recommendations": [{"fix": {"status": "proposed", "base_sha": "abc", "diff": None}}]},
        {"analyzed_at": "2026-10-28T13:15:00+01:00"},
        {"evidence": [{"source": "agnosticd_code", "timestamp": "", "message": "Configuration"}]},
        {"correlation": {"method": "custom", "identifiers": {"extension": True}}},
        {
            "causal_chain": [
                {"step": 1.0, "relationship": "contributing_factor", "statement": "Slow worker"}
            ]
        },
    ],
)
def test_summary_accepts_schema_defined_optional_fields(fields: dict[str, Any]) -> None:
    _assert_parity(AnalysisSummary, {**_summary(), **fields}, valid=True)


@pytest.mark.parametrize(
    "fields",
    [
        {"job_id": 101},
        {"root_cause": {"summary": "Failure", "category": "invalid", "confidence": "high"}},
        {"root_cause": {"summary": None, "category": "unknown", "confidence": "low"}},
        {"correlation": {"method": "none"}},
        {"correlation": {"method": "none", "identifiers": {"guid": None}}},
        {"evidence": []},
        {"evidence": [{"source": "related_job", "timestamp": "", "message": "Failure"}]},
        {"evidence": [{"source": "aap_job", "message": "Failure"}]},
        {"analyzed_at": None},
        {"recommendations": None},
        {"recommendations": [{"action": None}]},
        {"recommendations": [{"extra": "forbidden"}]},
        {"recommendations": [{"evidence_ref": -1}]},
        {"recommendations": [{"evidence_ref": True}]},
        {"recommendations": [{"evidence_ref": "0"}]},
        {"recommendations": [{"evidence_ref": 0.5}]},
        {"recommendations": [{"fix": {"status": "proposed"}}]},
        {"recommendations": [{"fix": {"base_sha": "abc"}}]},
        {"recommendations": [{"fix": {"status": "applied", "base_sha": "abc"}}]},
        {
            "recommendations": [
                {"fix": {"status": "proposed", "base_sha": "abc", "extra": "forbidden"}}
            ]
        },
        {"causal_chain": [{"step": 0, "relationship": "direct_cause", "statement": "Failure"}]},
        {"causal_chain": [{"step": True, "relationship": "direct_cause", "statement": "Failure"}]},
        {
            "causal_chain": [
                {"step": 1, "relationship": "direct_cause", "statement": "Failure", "extra": True}
            ]
        },
    ],
)
def test_summary_rejects_schema_invalid_payloads(fields: dict[str, Any]) -> None:
    _assert_parity(AnalysisSummary, {**_summary(), **fields}, valid=False)


@pytest.mark.parametrize("timestamp", ["2026-10-28", "2026-10-28T13:15:00", "invalid"])
def test_summary_preserves_existing_date_time_checker_behavior(timestamp: str) -> None:
    # jsonschema's date-time checker is available only with its format extras.
    valid = jsonschema.Draft7Validator.FORMAT_CHECKER.conforms(timestamp, "date-time")
    _assert_parity(AnalysisSummary, {**_summary(), "analyzed_at": timestamp}, valid=valid)


@pytest.mark.parametrize("status", ["proposed", "applied"])
def test_detail_accepts_fix_statuses_and_required_nulls(status: str) -> None:
    data = _detail()
    data["recommendations"][0]["fix"] = {"status": status, "base_sha": "abc"}
    data["platform"] = None
    data["causal_chain"][0]["step"] = 1.0
    _assert_parity(RootCauseDetail, data, valid=True)


def test_detail_allows_empty_required_arrays() -> None:
    _assert_parity(
        RootCauseDetail,
        {"summary": "Failure", "evidence": [], "causal_chain": [], "misidentifications": [], "recommendations": []},
        valid=True,
    )


@pytest.mark.parametrize(
    "section,field",
    [
        ("evidence", "timestamp"),
        ("evidence", "github_path"),
        ("causal_chain", "evidence_ref"),
        ("misidentifications", "evidence_ref"),
        ("recommendations", "github_path"),
        ("recommendations", "details"),
        ("recommendations", "evidence_ref"),
        ("recommendations", "fix"),
    ],
)
def test_detail_nullable_fields_are_still_required(section: str, field: str) -> None:
    data = _detail()
    del data[section][0][field]
    _assert_parity(RootCauseDetail, data, valid=False)


@pytest.mark.parametrize("section", ["evidence", "causal_chain", "misidentifications", "recommendations"])
def test_detail_rejects_extra_nested_properties(section: str) -> None:
    data = _detail()
    data[section][0]["extra"] = "forbidden"
    _assert_parity(RootCauseDetail, data, valid=False)


def test_detail_accepts_worked_example() -> None:
    example = json.loads(
        (Path(__file__).parent.parent / "docs/design/root-cause-example.json").read_text()
    )
    _assert_parity(RootCauseDetail, example, valid=True)
