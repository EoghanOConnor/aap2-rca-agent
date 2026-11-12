import json
from pathlib import Path

import pytest

from rca.analysis.correlator import (
    _analyze_correlation,
    _extract_unique_pods,
    _parse_ocp_logs,
    build_correlation_timeline,
)
from rca.models import CorrelationTimeline


@pytest.fixture
def correlator_data():
    data_path = Path(__file__).parent / "data" / "correlator_fixtures.json"
    with open(data_path, encoding="utf-8") as f:
        return json.load(f)


# --- _parse_ocp_logs tests ---


def test_parse_ocp_logs_valid_json(correlator_data):
    """Test parsing valid JSON _raw field."""
    # Convert _raw_object to _raw string to match expected input format
    raw_logs = []
    for log in correlator_data["valid_json_logs"]:
        new_log = log.copy()
        if "_raw_object" in new_log:
            new_log["_raw"] = json.dumps(new_log.pop("_raw_object"))
        raw_logs.append(new_log)

    parsed = _parse_ocp_logs(raw_logs)
    assert len(parsed) == 1
    assert parsed[0]["namespace"] == "test-ns"
    assert parsed[0]["pod_name"] == "test-pod"
    assert parsed[0]["container_name"] == "test-container"
    assert parsed[0]["message"] == "Pod started"


def test_parse_ocp_logs_invalid_json():
    """Test parsing invalid JSON _raw field (fallback)."""
    raw_logs = [{"_time": "2023-10-27T10:00:00Z", "_raw": "Not JSON"}]
    parsed = _parse_ocp_logs(raw_logs)
    assert len(parsed) == 1
    assert parsed[0]["message"] == "Not JSON"
    assert parsed[0]["namespace"] == ""


def test_parse_ocp_logs_truncation():
    """Test message truncation to 2000 chars."""
    long_msg = "a" * 3000
    raw_logs = [
        {
            "_time": "2023-10-27T10:00:00Z",
            "_raw": json.dumps({"message": long_msg}),
        }
    ]
    parsed = _parse_ocp_logs(raw_logs)
    assert len(parsed[0]["message"]) == 2000
    assert parsed[0]["message"] == "a" * 2000


def test_parse_ocp_logs_flat_keys(correlator_data):
    """Test extraction from flat keys if nested keys missing."""
    raw_logs = correlator_data["flat_keys_logs"]
    parsed = _parse_ocp_logs(raw_logs)
    assert parsed[0]["namespace"] == "flat-ns"
    assert parsed[0]["pod_name"] == "flat-pod"


# --- _extract_unique_pods tests ---


def test_extract_unique_pods_deduplication(correlator_data):
    """Test pod deduplication and container collection."""
    # Convert _raw_object to _raw string to match expected input format
    raw_logs = []
    for log in correlator_data["dedup_logs"]:
        raw_logs.append({"_raw": json.dumps(log["_raw_object"])})

    pods = _extract_unique_pods(raw_logs)
    assert len(pods) == 2
    pod1 = next(p for p in pods if p["pod_name"] == "pod-1")
    assert set(pod1["containers"]) == {"c1", "c2"}


def test_extract_unique_pods_skips_invalid():
    """Test skipping invalid JSON logs."""
    raw_logs = [{"_raw": "invalid json"}]
    pods = _extract_unique_pods(raw_logs)
    assert len(pods) == 0


# --- build_correlation_timeline tests ---


def test_build_correlation_timeline_structure(correlator_data):
    """Test timeline construction and model validation."""
    job_context = correlator_data["timeline_context"]
    splunk_logs = correlator_data["timeline_splunk_logs"]

    timeline = build_correlation_timeline(job_context, splunk_logs)

    CorrelationTimeline.model_validate(timeline)

    # Check events
    events = timeline["timeline_events"]
    assert len(events) == 2  # 1 failed task + 1 splunk error (info filtered)
    assert events[0]["source"] == "aap_job"
    assert events[1]["source"] == "splunk_ocp"
    assert events[0]["event_type"] == "task_failed"
    assert events[1]["event_type"] == "pod_error"

    # Check sorting
    assert events[0]["timestamp"] < events[1]["timestamp"]


# --- _analyze_correlation tests ---


def test_analyze_correlation_namespace_time_match(correlator_data):
    """Test high confidence match with namespace and time overlap."""
    import copy

    job_context = copy.deepcopy(correlator_data["analysis_context"])
    splunk_logs = copy.deepcopy(correlator_data["analysis_splunk_logs"])

    result = _analyze_correlation(job_context, splunk_logs)
    assert result["method"] == "namespace_time_match"
    assert result["confidence"] == "high"
    assert result["time_overlap"]["overlap_confirmed"] is True


def test_analyze_correlation_guid_time_match(correlator_data):
    """Test high confidence match with GUID (no namespace) and time overlap."""
    import copy

    job_context = copy.deepcopy(correlator_data["guid_match_context"])
    splunk_logs = copy.deepcopy(correlator_data["guid_match_splunk_logs"])

    result = _analyze_correlation(job_context, splunk_logs)
    assert result["method"] == "guid_time_match"
    assert result["confidence"] == "high"


def test_analyze_correlation_pod_name_match(correlator_data):
    """Test medium confidence match with matching pods but no time overlap."""
    import copy

    job_context = copy.deepcopy(correlator_data["pod_match_context"])
    splunk_logs = copy.deepcopy(correlator_data["pod_match_splunk_logs"])

    result = _analyze_correlation(job_context, splunk_logs)
    assert result["method"] == "pod_name_match"
    assert result["confidence"] == "medium"
    assert result["matching_pods"] == ["pod-1"]
    assert result["time_overlap"]["overlap_confirmed"] is False


def test_analyze_correlation_identifier_match(correlator_data):
    """Test low confidence match with identifiers but no time overlap or pod match."""
    import copy

    job_context = copy.deepcopy(correlator_data["identifier_match_context"])
    splunk_logs = copy.deepcopy(correlator_data["identifier_match_splunk_logs"])

    result = _analyze_correlation(job_context, splunk_logs)
    assert result["method"] == "identifier_match"
    assert result["confidence"] == "low"


def test_analyze_correlation_none(correlator_data):
    """Test no correlation."""
    import copy

    job_context = copy.deepcopy(correlator_data["no_match_context"])
    splunk_logs = copy.deepcopy(correlator_data["no_match_splunk_logs"])

    result = _analyze_correlation(job_context, splunk_logs)
    assert result["method"] == "none"
    assert result["confidence"] == "none"


def test_analyze_correlation_time_overlap_logic(correlator_data):
    """Test time overlap logic specifically."""
    import copy

    job_context = copy.deepcopy(correlator_data["time_overlap_context"])

    # Case 1: Disjoint (Splunk before Job)
    splunk_logs = copy.deepcopy(correlator_data["time_disjoint_before_logs"])
    result = _analyze_correlation(job_context, splunk_logs)
    assert result["time_overlap"]["overlap_confirmed"] is False

    # Case 2: Disjoint (Splunk after Job)
    splunk_logs = copy.deepcopy(correlator_data["time_disjoint_after_logs"])
    result = _analyze_correlation(job_context, splunk_logs)
    assert result["time_overlap"]["overlap_confirmed"] is False

    # Case 3: Overlap
    splunk_logs = copy.deepcopy(correlator_data["time_overlap_logs"])
    result = _analyze_correlation(job_context, splunk_logs)
    assert result["time_overlap"]["overlap_confirmed"] is True
