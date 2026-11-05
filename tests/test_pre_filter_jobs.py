"""Tests for reusable batch pre-filter functions and CLI behavior."""

import json
from io import StringIO
from unittest.mock import Mock

from rca.batch import pre_filter_jobs
from rca.batch.task_signature import TaskSignature


def _signature(error_message: str = "Connection reset by peer") -> TaskSignature:
    return TaskSignature(
        play="Setup",
        role="common",
        task="Install dependencies",
        task_action="yum",
        task_path="/roles/common/tasks/main.yml",
        error_message=error_message,
    )


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


def test_job_signal_accepts_a_signature_without_job_metadata() -> None:
    signal = pre_filter_jobs._job_signal(
        None,
        {"failed_tasks": [_signature().__dict__]},
    )

    assert signal is not None
    assert signal.catalog_item is None
    assert signal.text == "Connection reset by peer"
    assert signal.task_signature == _signature()

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


def test_pre_analysis_gate_preserves_ambiguous_catalog_guard_with_partial_signatures() -> None:
    job_metadata = {
        20: {
            "job_name": "RHPDS platform.widget.dev-20-create",
            "error_message": "Connection reset by peer",
        }
    }
    historical = [
        {
            "id": 90,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "canonical_result_id": 90,
            "error_message": "Connection reset by peer",
            "task_signature": _signature(),
        },
        {
            "id": 91,
            "catalog_item": "widget",
            "root_cause_category": "configuration",
            "canonical_result_id": 91,
            "error_message": "Connection reset by peer",
        },
    ]

    analyze_ids, skip_targets, dupes = pre_filter_jobs.pre_analysis_gate(
        [20],
        job_metadata,
        historical,
    )

    assert analyze_ids == [20]
    assert skip_targets == {}
    assert dupes == []


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


def test_pre_analysis_gate_clusters_same_task_signature_across_catalogs() -> None:
    job_metadata = {
        20: {
            "job_name": "RHPDS platform.widget.dev-20-create",
            "error_message": "Connection reset by peer",
        },
        21: {
            "job_name": "RHPDS platform.other.dev-21-create",
            "error_message": "Connection reset by peer",
        },
    }
    task_contexts = {
        20: {"failed_tasks": [_signature().__dict__]},
        21: {"failed_tasks": [_signature().__dict__]},
    }

    analyze_ids, _skip_targets, dupes = pre_filter_jobs.pre_analysis_gate(
        [21, 20],
        job_metadata,
        [],
        task_contexts=task_contexts,
    )

    assert analyze_ids == [20]
    assert dupes == [{"job_id": 21, "representative_job_id": 20}]


def test_pre_analysis_gate_keeps_same_task_with_different_errors_separate() -> None:
    job_metadata = {
        20: {"job_name": "RHPDS platform.widget.dev-20-create"},
        21: {"job_name": "RHPDS platform.widget.dev-21-create"},
    }
    task_contexts = {
        20: {"failed_tasks": [_signature("Connection reset by peer").__dict__]},
        21: {"failed_tasks": [_signature("Authentication failed").__dict__]},
    }

    analyze_ids, _skip_targets, dupes = pre_filter_jobs.pre_analysis_gate(
        [20, 21],
        job_metadata,
        [],
        task_contexts=task_contexts,
    )

    assert analyze_ids == [20, 21]
    assert dupes == []


def test_pre_analysis_gate_uses_task_signature_for_known_issue_matching() -> None:
    job_metadata = {20: {"job_name": "RHPDS platform.other.dev-20-create"}}
    task_contexts = {20: {"failed_tasks": [_signature().__dict__]}}
    historical = [
        {
            "id": 90,
            "job_id": 10,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "canonical_result_id": 90,
            "task_signature": _signature(),
        }
    ]

    analyze_ids, skip_targets, dupes = pre_filter_jobs.pre_analysis_gate(
        [20],
        job_metadata,
        historical,
        task_contexts=task_contexts,
    )

    assert analyze_ids == []
    assert skip_targets == {20: 90}
    assert dupes == []


def test_pre_analysis_gate_rejects_ambiguous_task_signature_history() -> None:
    job_metadata = {20: {"job_name": "RHPDS platform.widget.dev-20-create"}}
    task_contexts = {20: {"failed_tasks": [_signature().__dict__]}}
    historical = [
        {
            "id": 90,
            "job_id": 10,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "canonical_result_id": 90,
            "task_signature": _signature(),
        },
        {
            "id": 91,
            "job_id": 11,
            "catalog_item": "widget",
            "root_cause_category": "configuration",
            "canonical_result_id": 91,
            "task_signature": _signature(),
        },
    ]

    analyze_ids, skip_targets, dupes = pre_filter_jobs.pre_analysis_gate(
        [20],
        job_metadata,
        historical,
        task_contexts=task_contexts,
    )

    assert analyze_ids == [20]
    assert skip_targets == {}
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
