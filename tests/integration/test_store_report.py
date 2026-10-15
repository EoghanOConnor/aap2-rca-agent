"""Integration tests for store_report() against Postgres."""

from __future__ import annotations

import json

import pytest
from rca.batch.store_report import post_analysis_link, store_report
from tests.integration.conftest import RESULTS_TABLE, SOURCE_TABLE

pytestmark = pytest.mark.integration


def _config() -> dict[str, str]:
    return {"results_table": RESULTS_TABLE, "source_table": SOURCE_TABLE}


def _insert_source_job(conn, job_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {SOURCE_TABLE} (job_id, ai_processed) VALUES (%s, FALSE)",
            (job_id,),
        )
    conn.commit()


def test_store_report_persists_root_cause(db, tmp_path) -> None:
    _insert_source_job(db, 21)

    analysis_dir = tmp_path / ".analysis" / "21"
    analysis_dir.mkdir(parents=True)
    (analysis_dir / "step5_analysis_summary.json").write_text(
        json.dumps(
            {
                "evidence": [
                    {"source": "aap_job", "timestamp": "t", "message": "boom", "github_path": None}
                ],
                "causal_chain": [
                    {
                        "step": 1,
                        "relationship": "direct_cause",
                        "statement": "it broke",
                        "evidence_ref": 0,
                    }
                ],
                "misidentifications": [],
                "recommendations": [],
            }
        )
    )

    report = {
        "batch_id": "batch_test_store_report",
        "job_summaries": [
            {
                "job_id": "21",
                "status": "analyzed",
                "root_cause_category": "cloud_api",
                "root_cause_summary": "AWS throttled RunInstances",
                "confidence": "high",
                "catalog_item": "aws-blank-open-environment",
                "platform": "aws",
                "job_duration_seconds": 298,
                "analysis_path": str(analysis_dir),
            }
        ],
    }

    assert store_report(db, _config(), report) is True

    with db.cursor() as cur:
        cur.execute(
            f"SELECT root_cause, root_cause_category FROM {RESULTS_TABLE}"
            " WHERE batch_id = %s AND job_id = %s",
            ("batch_test_store_report", 21),
        )
        row = cur.fetchone()

    assert row is not None
    root_cause, category = row
    assert category == "cloud_api"
    assert root_cause["summary"] == "AWS throttled RunInstances"
    assert root_cause["evidence"][0]["job_id"] == "21"
    assert root_cause["causal_chain"][0]["relationship"] == "direct_cause"
    assert root_cause["misidentifications"] == []
    assert root_cause["recommendations"] == []


def test_store_report_upserts_on_conflict(db) -> None:
    _insert_source_job(db, 22)
    report = {
        "batch_id": "batch_test_conflict",
        "job_summaries": [
            {"job_id": "22", "status": "failed", "root_cause_category": None, "confidence": "low"}
        ],
    }

    assert store_report(db, _config(), report) is True
    assert store_report(db, _config(), report) is True

    with db.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {RESULTS_TABLE} WHERE batch_id = %s AND job_id = %s",
            ("batch_test_conflict", 22),
        )
        (count,) = cur.fetchone()

    assert count == 1


def test_post_analysis_link_persists_related_result(db) -> None:
    for job_id in (31, 32):
        _insert_source_job(db, job_id)

    historical_report = {
        "batch_id": "batch_test_historical",
        "job_summaries": [
            {
                "job_id": "31",
                "status": "analyzed",
                "root_cause_category": "infrastructure",
                "root_cause_summary": "Worker 7 timed out.",
                "confidence": "high",
                "catalog_item": "widget",
            }
        ],
    }
    assert store_report(db, _config(), historical_report, commit=False)
    historical_result_id = historical_report["job_summaries"][0]["result_id"]

    current_report = {
        "batch_id": "batch_test_current",
        "job_summaries": [
            {
                "job_id": "32",
                "status": "analyzed",
                "root_cause_category": "infrastructure",
                "root_cause_summary": "Worker 7 timed out.",
                "confidence": "high",
                "catalog_item": "widget",
                "historical_matches": [
                    {
                        "matched_result_id": historical_result_id,
                        "recurrence_count": 2,
                        "similarity_reasoning": "Both jobs time out in worker 7.",
                        "confidence": "high",
                    }
                ],
            }
        ],
    }
    assert store_report(db, _config(), current_report, commit=False)
    post_analysis_link(db, _config(), current_report, commit=False)
    db.commit()

    with db.cursor() as cur:
        cur.execute(
            f"SELECT id, related_result_id, related_result_confidence"
            f" FROM {RESULTS_TABLE} ORDER BY id"
        )
        rows = cur.fetchall()
        cur.execute(
            f"SELECT aap2_job_results_fk_id FROM {SOURCE_TABLE} WHERE job_id = 32"
        )
        (source_fk_id,) = cur.fetchone()

    assert rows[0] == (historical_result_id, None, None)
    assert rows[1] == (
        current_report["job_summaries"][0]["result_id"],
        historical_result_id,
        "high",
    )
    assert source_fk_id == historical_result_id


def test_post_analysis_link_merges_clusters_without_orphaned_anchors(db) -> None:
    for job_id in (41, 42, 43, 44, 45):
        _insert_source_job(db, job_id)

    def store_linked_batch(batch_id: str, job_ids: list[int]) -> list[int]:
        report = {
            "batch_id": batch_id,
            "job_summaries": [
                {
                    "job_id": str(job_id),
                    "status": "analyzed",
                    "root_cause_category": "infrastructure",
                    "root_cause_summary": "Worker 7 timed out.",
                    "confidence": "high",
                    "catalog_item": "widget",
                }
                for job_id in job_ids
            ],
            "cross_job_patterns": [
                {
                    "pattern": "worker timeout",
                    "jobs": [str(job_id) for job_id in job_ids],
                    "description": "Both jobs time out in worker 7.",
                    "source": "current_batch",
                    "confidence": "high",
                }
            ],
        }
        assert store_report(db, _config(), report, commit=False)
        post_analysis_link(db, _config(), report, commit=False)
        return [job["result_id"] for job in report["job_summaries"]]

    first_cluster = store_linked_batch("batch_test_cluster_a", [41, 42])
    second_cluster = store_linked_batch("batch_test_cluster_b", [43, 44])

    current_report = {
        "batch_id": "batch_test_cluster_merge",
        "job_summaries": [
            {
                "job_id": "45",
                "status": "analyzed",
                "root_cause_category": "infrastructure",
                "root_cause_summary": "Worker 7 timed out.",
                "confidence": "high",
                "catalog_item": "widget",
                "historical_matches": [
                    {
                        "matched_result_id": first_cluster[1],
                        "recurrence_count": 2,
                        "similarity_reasoning": "Both jobs time out in worker 7.",
                        "confidence": "high",
                    },
                    {
                        "matched_result_id": second_cluster[1],
                        "recurrence_count": 2,
                        "similarity_reasoning": "Both jobs time out in worker 7.",
                        "confidence": "high",
                    },
                ],
            }
        ],
    }
    assert store_report(db, _config(), current_report, commit=False)
    post_analysis_link(db, _config(), current_report, commit=False)
    db.commit()

    current_result_id = current_report["job_summaries"][0]["result_id"]
    result_ids = first_cluster + second_cluster + [current_result_id]
    canonical_anchor = min(first_cluster[0], second_cluster[0])
    with db.cursor() as cur:
        cur.execute(
            f"SELECT id, related_result_id FROM {RESULTS_TABLE}"
            " WHERE id = ANY(%s) ORDER BY id",
            (result_ids,),
        )
        rows = cur.fetchall()
        cur.execute(
            f"SELECT aap2_job_results_fk_id FROM {SOURCE_TABLE} WHERE job_id = 45"
        )
        (source_fk_id,) = cur.fetchone()

    assert rows == [
        (result_id, None if result_id == canonical_anchor else canonical_anchor)
        for result_id in sorted(result_ids)
    ]
    assert source_fk_id == canonical_anchor
