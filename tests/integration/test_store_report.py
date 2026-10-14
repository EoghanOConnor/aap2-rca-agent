"""Integration tests for store_report() against Postgres."""

from __future__ import annotations

import json

import pytest
from rca.batch.store_report import store_report
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
