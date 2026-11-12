#!/usr/bin/env python3
"""Assert the batch report shows a successful run.

Verifies structure and counts, not root-cause text: the LLM's content is
non-deterministic, but which jobs were analyzed is not.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <batch_report.json>", file=sys.stderr)
        return 2
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))

    analyzed = report["total_jobs_analyzed"]
    failed = report["total_jobs_failed"]
    requested = report["total_jobs_requested"]
    print(f"batch {report['batch_id']}: {requested} requested, {analyzed} analyzed, {failed} failed")
    for job in report.get("failed_analyses", []):
        print(f"  failed job {job['job_id']} at {job['stage']}: {job['error'][:120]}")

    # The seed set (ci/seed_test_db.py) is fixed and the pre-analysis gate is
    # deterministic: 900002 folds into 900001 (same catalog, identical error)
    # and 900004 into 900003 (cross-catalog, identical error), so exactly
    # these three jobs are analyzed. Exact values catch regressions in the
    # --since window, --limit, log staging and the dedup thresholds that a
    # ">= 1" assertion would let through.
    expected_analyzed = {"900001", "900003", "900005"}
    assert requested == 3, f"expected 3 jobs sent for analysis, got {requested}"
    assert analyzed == 3, f"expected 3 jobs analyzed, got {analyzed}"
    assert failed == 0, f"{failed} job(s) failed analysis"
    analyzed_ids = {str(job["job_id"]) for job in report["job_summaries"] if job["status"] == "analyzed"}
    assert analyzed_ids == expected_analyzed, (
        f"expected {sorted(expected_analyzed)} analyzed, got {sorted(analyzed_ids)}"
    )
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
