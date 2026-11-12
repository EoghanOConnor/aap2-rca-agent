"""Validate root_cause JSONB details with the Pydantic model."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from rca.models import RootCauseDetail


@pytest.fixture
def root_cause_example():
    example_path = Path(__file__).parent.parent / "docs" / "design" / "root-cause-example.json"
    with open(example_path) as f:
        return json.load(f)


def test_example_validates_against_model(root_cause_example):
    RootCauseDetail.model_validate(root_cause_example)


def test_required_arrays_may_be_empty():
    minimal = {
        "summary": "Something failed",
        "evidence": [],
        "causal_chain": [],
        "misidentifications": [],
        "recommendations": [],
    }
    RootCauseDetail.model_validate(minimal)


def test_fix_rejected_without_base_sha(root_cause_example):
    bad = json.loads(json.dumps(root_cause_example))
    del bad["recommendations"][0]["fix"]["base_sha"]
    with pytest.raises(ValidationError):
        RootCauseDetail.model_validate(bad)


def test_fix_rejected_without_status(root_cause_example):
    bad = json.loads(json.dumps(root_cause_example))
    del bad["recommendations"][0]["fix"]["status"]
    with pytest.raises(ValidationError):
        RootCauseDetail.model_validate(bad)


def test_null_fix_is_valid(root_cause_example):
    ok = json.loads(json.dumps(root_cause_example))
    ok["recommendations"][0]["fix"] = None
    RootCauseDetail.model_validate(ok)
