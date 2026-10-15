"""Tests for reusable batch pre-filter functions and CLI behavior."""

import json
from io import StringIO
from unittest.mock import Mock

from rca.batch import pre_filter_jobs


def test_pre_matched_entries_formats_and_preserves_input_order() -> None:
    recent_results = [
        {
            "id": 77,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "root_cause_summary": "Repeated connection reset",
            "error_message": "Connection reset by peer",
        }
    ]
    entries = pre_filter_jobs.pre_matched_entries(
        {12: 77, 11: 77},
        recent_results,
        [13, 11, 12],
    )

    assert entries == [
        {
            "job_id": 11,
            "matched_result_id": 77,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "match_reason": "pre_analysis_gate",
            "recent_result_summary": "Repeated connection reset",
        },
        {
            "job_id": 12,
            "matched_result_id": 77,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "match_reason": "pre_analysis_gate",
            "recent_result_summary": "Repeated connection reset",
        },
    ]


def test_pre_matched_entries_returns_empty_when_nothing_is_skipped() -> None:
    assert pre_filter_jobs.pre_matched_entries({}, [], [13, 11, 12]) == []

def test_pre_analysis_gate_clusters_duplicates_and_preserves_ambiguity_guard() -> None:
    job_metadata = {
        20: {
            "job_name": "RHPDS platform.widget.dev-20-create",
            "error_message": "Connection reset by peer",
        },
        21: {
            "job_name": "RHPDS platform.widget.dev-21-create",
            "error_message": "Connection reset by peer",
        },
    }
    historical = [
        {
            "id": 90,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "error_message": "Connection reset by peer",
        },
        {
            "id": 91,
            "catalog_item": "widget",
            "root_cause_category": "configuration",
            "error_message": "Connection reset by peer",
        },
    ]

    analyze_ids, skip_targets, dupes = pre_filter_jobs.pre_analysis_gate(
        [21, 20],
        job_metadata,
        historical,
    )

    assert analyze_ids == [20]
    assert skip_targets == {}
    assert dupes == [{"job_id": 21, "representative_job_id": 20}]


def test_pre_analysis_gate_does_not_cluster_generic_errors_across_catalogs() -> None:
    job_metadata = {
        20: {
            "job_name": "RHPDS platform.widget.dev-20-create",
            "error_message": "Failed",
        },
        21: {
            "job_name": "RHPDS platform.other.dev-21-create",
            "error_message": "Failed",
        },
    }

    analyze_ids, _skip_targets, dupes = pre_filter_jobs.pre_analysis_gate(
        [20, 21],
        job_metadata,
        [],
    )

    assert analyze_ids == [20, 21]
    assert dupes == []


def test_main_delegates_known_issue_filter_to_shared_helper(monkeypatch, capsys) -> None:
    job_ids = [13, 11, 12]
    connection = Mock()
    database_config = {
        "name": "rca",
        "user": "agent",
        "password": "secret",
        "source_table": "events",
        "results_table": "results",
    }
    recent_results = [
        {
            "id": 77,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "root_cause_summary": "Repeated connection reset",
        }
    ]
    job_metadata = {
        11: {
            "job_name": "RHPDS a.widget.dev-11-create",
            "error_message": "Connection reset by peer",
        },
        12: {
            "job_name": "RHPDS a.another-widget.dev-12-create",
            "error_message": "A completely unrelated authentication failure",
        },
    }
    expected = {
        "analyze": [13, 12],
        "pre_matched": [
            {
                "job_id": 11,
                "matched_result_id": 77,
                "catalog_item": "widget",
                "root_cause_category": "infrastructure",
                "match_reason": "pre_analysis_gate",
                "recent_result_summary": "Repeated connection reset",
            }
        ],
        "dupes": [],
    }
    fetch_recent_results = Mock(return_value=recent_results)
    fetch_job_metadata = Mock(return_value=job_metadata)
    pre_analysis_gate = Mock(return_value=([13, 12], {11: 77}, []))

    monkeypatch.setattr(pre_filter_jobs.sys, "stdin", StringIO("13\n11\n12\n"))
    monkeypatch.setattr(
        pre_filter_jobs, "load_database_config", Mock(return_value=database_config)
    )
    monkeypatch.setattr(pre_filter_jobs, "connect_db", Mock(return_value=connection))
    monkeypatch.setattr(pre_filter_jobs, "fetch_recent_results", fetch_recent_results)
    monkeypatch.setattr(pre_filter_jobs, "fetch_job_metadata", fetch_job_metadata)
    monkeypatch.setattr(pre_filter_jobs, "pre_analysis_gate", pre_analysis_gate)

    assert pre_filter_jobs.main(["--lookback-hours", "8"]) == 0

    fetch_recent_results.assert_called_once_with(connection, "results", "events", 8)
    fetch_job_metadata.assert_called_once_with(connection, "events", job_ids)
    pre_analysis_gate.assert_called_once_with(job_ids, job_metadata, recent_results)
    connection.close.assert_called_once_with()
    assert json.loads(capsys.readouterr().out) == expected


def test_main_includes_intra_batch_dupes_in_output(monkeypatch, capsys) -> None:
    connection = Mock()
    database_config = {
        "name": "rca",
        "user": "agent",
        "password": "secret",
        "source_table": "events",
        "results_table": "results",
    }
    pre_analysis_gate = Mock(
        return_value=([13], {}, [{"job_id": 12, "representative_job_id": 13}])
    )

    monkeypatch.setattr(pre_filter_jobs.sys, "stdin", StringIO("13\n12\n"))
    monkeypatch.setattr(
        pre_filter_jobs, "load_database_config", Mock(return_value=database_config)
    )
    monkeypatch.setattr(pre_filter_jobs, "connect_db", Mock(return_value=connection))
    monkeypatch.setattr(pre_filter_jobs, "fetch_recent_results", Mock(return_value=[]))
    monkeypatch.setattr(pre_filter_jobs, "fetch_job_metadata", Mock(return_value={}))
    monkeypatch.setattr(pre_filter_jobs, "pre_analysis_gate", pre_analysis_gate)

    assert pre_filter_jobs.main([]) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["dupes"] == [{"job_id": 12, "representative_job_id": 13}]
