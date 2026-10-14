"""Tests for storing batch reports with psycopg2 mapping-style rows."""

import json
from unittest.mock import MagicMock

from psycopg2 import sql

from rca.batch import store_report


def _connection_and_cursor():
    connection = MagicMock()
    cursor_context = MagicMock()
    cursor = cursor_context.__enter__.return_value
    cursor.connection = connection
    connection.cursor.return_value = cursor_context
    return connection, cursor


def test_find_match_accepts_mapping_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        store_report,
        "known_issue_active_sql",
        lambda _conn, *, table: sql.SQL("TRUE"),
    )
    cursor = MagicMock()
    cursor.connection = MagicMock()
    cursor.fetchall.return_value = [
        {"id": 42, "root_cause_summary": "A worker ran out of capacity"}
    ]

    match_id = store_report.find_match(
        cursor,
        "results",
        {
            "job_id": "123",
            "root_cause_category": "infrastructure",
            "catalog_item": "widget",
            "root_cause_summary": "A worker ran out of capacity",
        },
    )

    assert match_id == 42


def test_store_report_reads_mapping_insert_result(monkeypatch) -> None:
    monkeypatch.setattr(
        store_report,
        "known_issue_active_sql",
        lambda _conn, *, table: sql.SQL("TRUE"),
    )
    connection, cursor = _connection_and_cursor()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = {"id": 55}

    assert store_report.store_report(
        connection,
        {"results_table": "results", "source_table": "events"},
        {
            "batch_id": "batch_test",
            "job_summaries": [
                {
                    "job_id": "123",
                    "status": "analyzed",
                    "root_cause_category": "infrastructure",
                    "root_cause_summary": "A worker ran out of capacity",
                    "confidence": "high",
                    "catalog_item": "widget",
                }
            ],
        },
    )

    assert (55, "123") in [call.args[1] for call in cursor.execute.call_args_list]
    connection.commit.assert_called_once_with()


def test_link_intra_batch_dupes_reads_mapping_result_row() -> None:
    connection, cursor = _connection_and_cursor()
    cursor.fetchone.return_value = {"aap2_job_results_fk_id": 91}

    linked = store_report.link_intra_batch_dupes(
        connection,
        {"source_table": "events"},
        [{"job_id": "222", "representative_job_id": "111"}],
    )

    assert linked == 1
    assert (91, "222") in [call.args[1] for call in cursor.execute.call_args_list]
    connection.commit.assert_called_once_with()


def test_store_report_assigns_pattern_id_from_result_ids(monkeypatch) -> None:
    monkeypatch.setattr(
        store_report,
        "known_issue_active_sql",
        lambda _conn, *, table: sql.SQL("TRUE"),
    )
    connection, cursor = _connection_and_cursor()
    cursor.fetchall.return_value = []
    cursor.fetchone.side_effect = [{"id": 12}, {"id": 11}]
    report = {
        "batch_id": "batch_test",
        "job_summaries": [
            {"job_id": "123", "status": "analyzed", "confidence": "high"},
            {"job_id": "124", "status": "analyzed", "confidence": "high"},
        ],
        "cross_job_patterns": [
            {
                "pattern": "worker timeout",
                "jobs": ["123", "124"],
                "description": "Both jobs time out in worker 7.",
                "source": "current_batch",
            }
        ],
    }

    assert store_report.store_report(
        connection,
        {"results_table": "results", "source_table": "events"},
        report,
    )

    assert report["job_summaries"][0]["result_id"] == 12
    assert report["job_summaries"][1]["result_id"] == 11
    assert report["cross_job_patterns"][0]["pattern_id"] == "11"
    connection.commit.assert_called_once_with()


def test_store_cross_patterns_persists_derived_pattern_id() -> None:
    connection, cursor = _connection_and_cursor()
    report = {
        "batch_id": "batch_test",
        "job_summaries": [
            {"job_id": "123", "status": "analyzed", "result_id": 42},
            {"job_id": "124", "status": "analyzed", "result_id": 43},
        ],
        "cross_job_patterns": [
            {
                "pattern": "worker timeout",
                "jobs": ["123", "124"],
                "description": "Both jobs time out in worker 7.",
                "source": "current_batch",
                "pattern_id": "42",
            }
        ],
    }

    store_report.store_cross_patterns(connection, {"results_table": "results"}, report)

    execute_params = [call.args[1] for call in cursor.execute.call_args_list]
    assert ("42", "Both jobs time out in worker 7.", 42) in execute_params
    assert ("42", "Both jobs time out in worker 7.", 43) in execute_params
    connection.commit.assert_called_once_with()


# --- build_root_cause() ---


def _write_step5(tmp_path, data):
    analysis_dir = tmp_path / ".analysis" / "21"
    analysis_dir.mkdir(parents=True)
    (analysis_dir / "step5_analysis_summary.json").write_text(json.dumps(data))
    return str(analysis_dir)


def test_build_root_cause_without_analysis_path_defaults_to_empty_arrays() -> None:
    job = {
        "job_id": "21",
        "root_cause_summary": "Something failed",
        "status": "failed",
    }
    root_cause = store_report.build_root_cause(job, "21")

    assert root_cause["summary"] == "Something failed"
    assert root_cause["evidence"] == []
    assert root_cause["causal_chain"] == []
    assert root_cause["misidentifications"] == []
    assert root_cause["recommendations"] == []


def test_build_root_cause_with_missing_step5_file_defaults_to_empty_arrays(tmp_path) -> None:
    job = {
        "job_id": "21",
        "root_cause_summary": "Something failed",
        "analysis_path": str(tmp_path / "does-not-exist"),
    }
    root_cause = store_report.build_root_cause(job, "21")

    assert root_cause["evidence"] == []
    assert root_cause["causal_chain"] == []
    assert root_cause["misidentifications"] == []
    assert root_cause["recommendations"] == []


def test_build_root_cause_reads_step5_analysis(tmp_path) -> None:
    analysis_path = _write_step5(
        tmp_path,
        {
            "evidence": [
                {"source": "aap_job", "timestamp": "t1", "message": "boom", "github_path": None}
            ],
            "causal_chain": [
                {"step": 1, "relationship": "direct_cause", "statement": "it broke", "evidence_ref": 0}
            ],
            "misidentifications": [
                {
                    "theory": "DNS",
                    "why_suspected": "looked like DNS",
                    "why_ruled_out": "it was not DNS",
                    "evidence_ref": 0,
                }
            ],
            "recommendations": [
                {"priority": "high", "action": "fix it", "github_path": "o/r:f.yml", "details": "d"}
            ],
        },
    )
    job = {
        "job_id": "21",
        "root_cause_summary": "Something failed",
        "platform": "aws",
        "failing_role": "aws_instance_create",
        "failing_github_path": "o/r:f.yml:1",
        "analysis_path": analysis_path,
    }

    root_cause = store_report.build_root_cause(job, "21")

    assert root_cause["platform"] == "aws"
    assert root_cause["failing_role"] == "aws_instance_create"
    assert root_cause["evidence"] == [
        {"source": "aap_job", "job_id": "21", "timestamp": "t1", "message": "boom", "github_path": None}
    ]
    assert root_cause["causal_chain"][0]["statement"] == "it broke"
    assert root_cause["misidentifications"][0]["why_ruled_out"] == "it was not DNS"
    rec = root_cause["recommendations"][0]
    assert rec["action"] == "fix it"
    assert rec["fix"] is None


def test_build_root_cause_backfills_evidence_job_id(tmp_path) -> None:
    analysis_path = _write_step5(
        tmp_path,
        {"evidence": [{"source": "splunk_ocp", "message": "m", "timestamp": "t", "github_path": None}]},
    )
    job = {"job_id": "21", "analysis_path": analysis_path}

    root_cause = store_report.build_root_cause(job, "21")

    assert root_cause["evidence"][0]["job_id"] == "21"


def test_build_root_cause_normalizes_incomplete_fix(tmp_path) -> None:
    analysis_path = _write_step5(
        tmp_path,
        {
            "recommendations": [
                {"priority": "high", "action": "fix it", "fix": {"status": "proposed"}},
                {
                    "priority": "medium",
                    "action": "also fix",
                    "fix": {"status": "proposed", "base_sha": "abc123", "diff": "d", "pr_link": None},
                },
            ]
        },
    )
    job = {"job_id": "21", "analysis_path": analysis_path}

    root_cause = store_report.build_root_cause(job, "21")

    assert root_cause["recommendations"][0]["fix"] is None
    assert root_cause["recommendations"][1]["fix"] == {
        "status": "proposed",
        "base_sha": "abc123",
        "diff": "d",
        "pr_link": None,
    }


def test_build_root_cause_normalizes_causal_chain_and_misidentifications(tmp_path) -> None:
    analysis_path = _write_step5(
        tmp_path,
        {
            "causal_chain": [
                {"step": 1, "relationship": "direct_cause", "statement": "it broke", "unexpected": "x"}
            ],
            "misidentifications": [
                {"theory": "DNS", "why_suspected": "s", "why_ruled_out": "r", "unexpected": "x"}
            ],
        },
    )
    job = {"job_id": "21", "analysis_path": analysis_path}

    root_cause = store_report.build_root_cause(job, "21")

    assert root_cause["causal_chain"] == [
        {"step": 1, "relationship": "direct_cause", "statement": "it broke", "evidence_ref": None}
    ]
    assert root_cause["misidentifications"] == [
        {"theory": "DNS", "why_suspected": "s", "why_ruled_out": "r", "evidence_ref": None}
    ]


def test_find_match_handles_null_historical_summary(monkeypatch, capsys) -> None:
    from psycopg2 import sql

    monkeypatch.setattr(
        store_report,
        "known_issue_active_sql",
        lambda _conn, *, table: sql.SQL("TRUE"),
    )
    cursor = MagicMock()
    cursor.connection = MagicMock()
    cursor.fetchall.return_value = [{"id": 7, "root_cause_summary": None}]

    match_id = store_report.find_match(
        cursor,
        "results",
        {
            "job_id": "123",
            "root_cause_category": "infrastructure",
            "catalog_item": "widget",
            "root_cause_summary": "",
        },
    )

    assert match_id == 7
