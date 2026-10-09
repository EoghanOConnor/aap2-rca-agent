"""Validate the root_cause JSONB schema against its worked example."""

import json
from importlib.resources import files
from pathlib import Path

import jsonschema
import pytest

schemas_path = files("rca").joinpath("schemas")


@pytest.fixture
def root_cause_schema():
    schema_path = schemas_path.joinpath("root_cause.schema.json")
    with open(schema_path) as f:
        return json.load(f)


@pytest.fixture
def root_cause_example():
    example_path = Path(__file__).parent.parent / "docs" / "design" / "root-cause-example.json"
    with open(example_path) as f:
        return json.load(f)


def test_example_validates_against_schema(root_cause_schema, root_cause_example):
    jsonschema.validate(instance=root_cause_example, schema=root_cause_schema)


def test_required_arrays_may_be_empty(root_cause_schema):
    minimal = {
        "summary": "Something failed",
        "evidence": [],
        "causal_chain": [],
        "misidentifications": [],
        "recommendations": [],
    }
    jsonschema.validate(instance=minimal, schema=root_cause_schema)


def test_fix_rejected_without_base_sha(root_cause_schema, root_cause_example):
    bad = json.loads(json.dumps(root_cause_example))
    del bad["recommendations"][0]["fix"]["base_sha"]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=bad, schema=root_cause_schema)


def test_fix_rejected_without_status(root_cause_schema, root_cause_example):
    bad = json.loads(json.dumps(root_cause_example))
    del bad["recommendations"][0]["fix"]["status"]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=bad, schema=root_cause_schema)


def test_null_fix_is_valid(root_cause_schema, root_cause_example):
    ok = json.loads(json.dumps(root_cause_example))
    ok["recommendations"][0]["fix"] = None
    jsonschema.validate(instance=ok, schema=root_cause_schema)
