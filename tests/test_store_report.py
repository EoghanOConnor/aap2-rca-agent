"""Tests for storing batch reports with psycopg2 mapping-style rows."""

import json
from unittest.mock import MagicMock

from rca.batch import store_report


def _connection_and_cursor():
    connection = MagicMock()
    cursor_context = MagicMock()
    cursor = cursor_context.__enter__.return_value
    cursor.connection = connection
    connection.cursor.return_value = cursor_context
    return connection, cursor


def test_store_report_reads_mapping_insert_result() -> None:
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
                    "historical_matches": [
                        {
                            "matched_result_id": 99,
                            "similarity_reasoning": "Same worker capacity issue.",
                            "confidence": "high",
                        }
                    ],
                }
            ],
        },
    )

    assert (55, "123") in [call.args[1] for call in cursor.execute.call_args_list]
    assert (99, "123") not in [call.args[1] for call in cursor.execute.call_args_list]
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


def test_store_report_assigns_pattern_id_from_result_ids() -> None:
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


def test_post_analysis_link_clears_a_stale_fallback_pattern_id_on_rejection() -> None:
    # store_report() stamps a fallback pattern_id = min(result_ids) onto any
    # pattern lacking one before post_analysis_link validates it. If validation
    # then rejects the pattern (here: incompatible root_cause_category), that
    # fallback must not survive and misrepresent an unlinked pattern as linked.
    connection, cursor = _connection_and_cursor()
    cursor.fetchall.return_value = []
    report = {
        "batch_id": "batch_test",
        "job_summaries": [
            {
                "job_id": "123",
                "status": "analyzed",
                "result_id": 42,
                "catalog_item": "widget",
                "root_cause_category": "infrastructure",
                "root_cause_summary": "Worker 7 timed out.",
            },
            {
                "job_id": "124",
                "status": "analyzed",
                "result_id": 43,
                "catalog_item": "widget",
                "root_cause_category": "application_bug",
                "root_cause_summary": "Worker 7 timed out.",
            },
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

    store_report.post_analysis_link(
        connection, {"results_table": "results", "source_table": "events"}, report
    )

    assert "pattern_id" not in report["cross_job_patterns"][0]


def test_post_analysis_link_persists_derived_pattern_id() -> None:
    connection, cursor = _connection_and_cursor()
    result_rows = [
        {
            "id": 42,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "root_cause_summary": "Worker 7 timed out.",
            "related_result_id": None,
            "related_result_confidence": None,
        },
        {
            "id": 43,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "root_cause_summary": "Worker 7 timed out.",
            "related_result_id": None,
            "related_result_confidence": None,
        },
    ]
    cursor.fetchall.side_effect = [result_rows, result_rows]
    report = {
        "batch_id": "batch_test",
        "job_summaries": [
            {
                "job_id": "123",
                "status": "analyzed",
                "result_id": 42,
                "catalog_item": "widget",
                "root_cause_category": "infrastructure",
                "root_cause_summary": "Worker 7 timed out.",
            },
            {
                "job_id": "124",
                "status": "analyzed",
                "result_id": 43,
                "catalog_item": "widget",
                "root_cause_category": "infrastructure",
                "root_cause_summary": "Worker 7 timed out.",
            },
        ],
        "cross_job_patterns": [
            {
                "pattern": "worker timeout",
                "jobs": ["123", "124"],
                "description": "Both jobs time out in worker 7.",
                "source": "current_batch",
                "pattern_id": "42",
                "confidence": "high",
            }
        ],
    }

    store_report.post_analysis_link(
        connection, {"results_table": "results", "source_table": "events"}, report
    )

    update_params = [call.args[1] for call in cursor.execute.call_args_list if len(call.args) > 1]
    assert any(
        params == ([42, 43], [42, 42], ["high", "high"])
        for params in update_params
    )
    assert report["cross_job_patterns"][0]["pattern_id"] == "42"
    # Job 124 is linked only through the cross-job pattern (no historical
    # match), but its source-table FK must still be repointed to the anchor.
    assert (42, "124") in update_params
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


def test_post_analysis_link_updates_a_null_anchor_historical_row() -> None:
    connection, cursor = _connection_and_cursor()
    target_row = {
        "id": 50,
        "catalog_item": "widget",
        "root_cause_category": "infrastructure",
        "root_cause_summary": "Worker 7 timed out.",
        "related_result_id": None,
        "related_result_confidence": None,
    }
    current_row = {
        "id": 100,
        "catalog_item": "widget",
        "root_cause_category": "infrastructure",
        "root_cause_summary": "Worker 7 timed out.",
        "related_result_id": None,
        "related_result_confidence": None,
    }
    cursor.fetchall.side_effect = [
        [target_row],
        [target_row, current_row],
        [current_row],
    ]
    report = {
        "batch_id": "batch_test",
        "job_summaries": [
            {
                "job_id": "123",
                "status": "analyzed",
                "result_id": 100,
                "catalog_item": "widget",
                "root_cause_category": "infrastructure",
                "root_cause_summary": "Worker 7 timed out.",
                "historical_matches": [
                    {
                        "matched_result_id": 50,
                        "recurrence_count": 2,
                        "similarity_reasoning": "Both jobs time out in worker 7.",
                        "confidence": "high",
                    }
                ],
            }
        ],
    }

    store_report.post_analysis_link(
        connection, {"results_table": "results", "source_table": "events"}, report
    )

    update_params = [call.args[1] for call in cursor.execute.call_args_list if len(call.args) > 1]
    assert any(
        params == ([50, 100], [50, 50], ["high", "high"])
        for params in update_params
    )
    assert (50, "123") in update_params
    assert report["job_summaries"][0]["historical_matches"][0]["pattern_id"] == "50"


def test_post_analysis_link_does_not_claim_a_missing_result() -> None:
    connection, cursor = _connection_and_cursor()
    cursor.fetchall.return_value = []
    report = {
        "batch_id": "batch_test",
        "job_summaries": [
            {
                "job_id": "123",
                "status": "analyzed",
                "result_id": 100,
                "catalog_item": "widget",
                "root_cause_category": "infrastructure",
                "root_cause_summary": "Worker 7 timed out.",
                "historical_matches": [
                    {
                        "matched_result_id": 50,
                        "recurrence_count": 2,
                        "similarity_reasoning": "Both jobs time out in worker 7.",
                        "confidence": "high",
                    }
                ],
            }
        ],
    }

    store_report.post_analysis_link(
        connection, {"results_table": "results", "source_table": "events"}, report
    )

    update_params = [call.args[1] for call in cursor.execute.call_args_list if len(call.args) > 1]
    assert update_params == [([50],)]
    assert "pattern_id" not in report["job_summaries"][0]["historical_matches"][0]


def test_post_analysis_link_drops_whole_pattern_when_a_member_row_is_missing() -> None:
    connection, cursor = _connection_and_cursor()
    row_1 = {
        "id": 1,
        "catalog_item": "widget",
        "root_cause_category": "infrastructure",
        "root_cause_summary": "Worker 7 timed out.",
        "related_result_id": None,
        "related_result_confidence": None,
    }
    row_3 = {
        "id": 3,
        "catalog_item": "widget",
        "root_cause_category": "infrastructure",
        "root_cause_summary": "Worker 7 timed out.",
        "related_result_id": None,
        "related_result_confidence": None,
    }
    # Job 2's result row is missing from the results table (deleted/raced).
    cursor.fetchall.return_value = [row_1, row_3]
    report = {
        "batch_id": "batch_test",
        "job_summaries": [
            {"job_id": "1", "status": "analyzed", "result_id": 1},
            {"job_id": "2", "status": "analyzed", "result_id": 2},
            {"job_id": "3", "status": "analyzed", "result_id": 3},
        ],
        "cross_job_patterns": [
            {
                "pattern": "worker timeout",
                "jobs": ["1", "2", "3"],
                "description": "All three jobs time out in worker 7.",
                "source": "current_batch",
                "confidence": "high",
            }
        ],
    }

    store_report.post_analysis_link(
        connection, {"results_table": "results", "source_table": "events"}, report
    )

    update_statements = [
        call.args[0]
        for call in cursor.execute.call_args_list
        if "SET related_result_id" in str(call.args[0])
    ]
    assert update_statements == []
    assert "pattern_id" not in report["cross_job_patterns"][0]


def test_post_analysis_link_applies_strict_threshold_to_deterministic_fallback_matches() -> None:
    connection, cursor = _connection_and_cursor()
    target_row = {
        "id": 50,
        "catalog_item": None,
        "root_cause_category": "infrastructure",
        "root_cause_summary": (
            "Worker pool exhausted during provisioning"
        ),
        "related_result_id": None,
        "related_result_confidence": None,
    }
    cursor.fetchall.return_value = [target_row]
    report = {
        "batch_id": "batch_test",
        "job_summaries": [
            {
                "job_id": "123",
                "status": "analyzed",
                "result_id": 100,
                "catalog_item": None,
                "root_cause_category": "infrastructure",
                # ~0.51 SequenceMatcher similarity to target_row's summary: clears
                # the lenient semantic "high" threshold (0.50) but not the strict
                # deterministic "low" threshold (0.55) that should apply here.
                "root_cause_summary": (
                    "Worker pool exhausted during provisioning"
                    + " x" * 34
                ),
                "historical_matches": [
                    {
                        "matched_result_id": 50,
                        "recurrence_count": 1,
                        "similarity_reasoning": "Deterministic root-cause summary similarity is 51%.",
                        "confidence": "high",
                        "match_source": "deterministic_fallback",
                        "evidence": [
                            {
                                "statement_type": "observed",
                                "description": "The root-cause summaries are 51% similar.",
                                "source": "deterministic_fallback",
                            }
                        ],
                    }
                ],
            }
        ],
    }

    store_report.post_analysis_link(
        connection, {"results_table": "results", "source_table": "events"}, report
    )

    update_statements = [
        call.args[0]
        for call in cursor.execute.call_args_list
        if "SET related_result_id" in str(call.args[0])
    ]
    assert update_statements == []
    assert "pattern_id" not in report["job_summaries"][0]["historical_matches"][0]


def test_post_analysis_link_does_not_trust_a_spoofed_evidence_source() -> None:
    # The semantic aggregator's evidence[].source is free text the LLM controls
    # (no enum in its schema); only the code-set match_source field may gate the
    # strict deterministic threshold. A match claiming source="deterministic_fallback"
    # in its evidence, but lacking match_source, must still be treated as a genuine
    # semantic match and use the lenient threshold.
    connection, cursor = _connection_and_cursor()
    target_row = {
        "id": 50,
        "catalog_item": None,
        "root_cause_category": "infrastructure",
        "root_cause_summary": "Worker pool exhausted during provisioning",
        "related_result_id": None,
        "related_result_confidence": None,
    }
    current_row = {**target_row, "id": 100}
    cursor.fetchall.side_effect = [
        [target_row],
        [target_row, current_row],
        [current_row],
    ]
    report = {
        "batch_id": "batch_test",
        "job_summaries": [
            {
                "job_id": "123",
                "status": "analyzed",
                "result_id": 100,
                "catalog_item": None,
                "root_cause_category": "infrastructure",
                "root_cause_summary": "Worker pool exhausted during provisioning" + " x" * 34,
                "historical_matches": [
                    {
                        "matched_result_id": 50,
                        "recurrence_count": 1,
                        "similarity_reasoning": "The aggregator judged these related.",
                        "confidence": "high",
                        "evidence": [
                            {
                                "statement_type": "observed",
                                "description": "Spoofed source field.",
                                "source": "deterministic_fallback",
                            }
                        ],
                    }
                ],
            }
        ],
    }

    store_report.post_analysis_link(
        connection, {"results_table": "results", "source_table": "events"}, report
    )

    update_statements = [
        call.args[0]
        for call in cursor.execute.call_args_list
        if "SET related_result_id" in str(call.args[0])
    ]
    assert len(update_statements) == 1
    assert report["job_summaries"][0]["historical_matches"][0]["pattern_id"] == "50"
