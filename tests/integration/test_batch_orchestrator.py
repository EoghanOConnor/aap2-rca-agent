"""PostgreSQL integration tests for the batch orchestrator persistence path."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import psycopg2.extras
import pytest

from rca.batch import orchestrator
from tests.integration.conftest import RESULTS_TABLE, SOURCE_TABLE

pytestmark = pytest.mark.integration


class _SingleConnectionPool:
    """Let the orchestrator borrow one already-open integration-test connection."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def getconn(self) -> Any:
        return self._connection

    def putconn(self, connection: Any, *, close: bool = False) -> None:
        assert connection is self._connection


def _config(tmp_path: Path) -> orchestrator.Config:
    return orchestrator.Config.from_env(
        environment={
            "RCA_STATE_DIR": str(tmp_path),
            "RCA_MAX_PARALLEL_JOBS": "3",
            "SOURCE_DB_NAME": "rca_test",
            "SOURCE_DB_USER": "rca_test",
            "SOURCE_DB_PASSWORD": "rca_test",
            "SOURCE_DB_TABLE": SOURCE_TABLE,
            "SOURCE_DB_RESULT_TABLE": RESULTS_TABLE,
            "JUMPBOX_URI": "agent@jumpbox.example.test",
        }
    )


def test_run_batch_limit_three_persists_result_columns(
    db: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    job_ids = [304, 303, 302]
    finished_at = datetime(2025, 12, 31, 23, 45, tzinfo=timezone.utc)
    for job_id in (301, *job_ids):
        with db.cursor() as cur:
            cur.execute(
                f"INSERT INTO {SOURCE_TABLE} (job_id, job_finished, ai_processed) "
                "VALUES (%s, %s, FALSE)",
                (job_id, finished_at),
            )
    db.commit()
    monkeypatch.setattr(db, "cursor_factory", psycopg2.extras.RealDictCursor)

    config = _config(tmp_path)
    executions = [
        orchestrator.JobExecution(
            str(job_id),
            "completed",
            1,
            summary={
                "root_cause": {
                    "summary": "Worker 7 timed out while provisioning the platform.",
                    "category": "infrastructure",
                    "confidence": "high",
                },
                "recommendations": [],
            },
        )
        for job_id in job_ids
    ]
    cross_patterns = [
        {
            "pattern": "worker timeout",
            "jobs": [str(job_id) for job_id in job_ids],
            "description": "All three jobs timed out in worker 7.",
            "source": "current_batch",
            "confidence": "high",
        }
    ]

    analyze_jobs = AsyncMock(return_value=executions)
    monkeypatch.setattr(orchestrator, "fetch_job_metadata", Mock(return_value={}))
    monkeypatch.setattr(orchestrator, "fetch_recent_results", Mock(return_value=[]))
    monkeypatch.setattr(orchestrator, "fetch_known_issues", Mock(return_value=[]))
    monkeypatch.setattr(
        orchestrator,
        "pre_analysis_gate",
        Mock(return_value=(job_ids, {}, [])),
    )
    monkeypatch.setattr(orchestrator, "_analyze_jobs", analyze_jobs)
    monkeypatch.setattr(
        orchestrator,
        "_aggregate_semantics",
        AsyncMock(return_value=(cross_patterns, None)),
    )
    monkeypatch.setattr(orchestrator, "_log_mlflow_usage", Mock())
    monkeypatch.setattr(orchestrator, "ensure_jumpbox_alias", Mock())

    pool = _SingleConnectionPool(db)
    assert (
        orchestrator.run_batch(
            config,
            limit=3,
            pool=pool,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
            cwd=tmp_path,
        )
        == 0
    )

    assert analyze_jobs.await_args.args[0] == job_ids

    with db.cursor() as cur:
        cur.execute(
            f"SELECT job_id, id, root_cause_category, confidence,"
            f" related_result_id, related_result_confidence"
            f" FROM {RESULTS_TABLE} WHERE batch_id = %s",
            ("batch_20260101_000000",),
        )
        rows = cur.fetchall()
        cur.execute(
            f"SELECT ai_processed FROM {SOURCE_TABLE} WHERE job_id = 301"
        )
        unselected_row = cur.fetchone()

    assert len(rows) == 3
    by_job = {
        str(row["job_id"]): (
            row["id"],
            row["root_cause_category"],
            row["confidence"],
            row["related_result_id"],
            row["related_result_confidence"],
        )
        for row in rows
    }
    assert set(by_job) == {"302", "303", "304"}
    anchor_id = by_job["304"][0]
    for job_id, values in by_job.items():
        result_id, category, confidence, related_id, related_confidence = values
        assert result_id is not None
        assert category == "infrastructure"
        assert confidence == "high"
        if job_id == "304":
            assert related_id is None
            assert related_confidence is None
        else:
            assert related_id == anchor_id
            assert related_confidence == "high"
    assert unselected_row["ai_processed"] is False
