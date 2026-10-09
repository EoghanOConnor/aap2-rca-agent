"""Store batch RCA JSON reports into a results table on the source database."""

from __future__ import annotations

import argparse
import difflib
import glob
import json
import os
import sys
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from typing import Any

import jsonschema
import psycopg2
import psycopg2.extras
import psycopg2.sql

from rca.config import load_database_config
from rca.database import connect_db, known_issue_active_sql

MATCH_THRESHOLD = 0.85
LOOKBACK_HOURS = 4

_ROOT_CAUSE_SCHEMA: dict[str, Any] = json.loads(
    files("rca").joinpath("schemas").joinpath("root_cause.schema.json").read_text(encoding="utf-8")
)


def _load_step5(analysis_path: str | None, jid: str) -> dict[str, Any] | None:
    """Read step5_analysis_summary.json from the job's analysis directory, if available."""
    if not analysis_path:
        return None
    path = os.path.join(analysis_path, "step5_analysis_summary.json")
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"[WARN] job {jid}: could not read step5 analysis ({path}): {e}", file=sys.stderr)
        return None


def _normalize_evidence(raw: list[Any] | None, jid: str) -> list[dict[str, Any]]:
    evidence = []
    for item in raw or []:
        if not isinstance(item, dict):
            continue
        evidence.append(
            {
                "source": item.get("source"),
                "job_id": item.get("job_id") or jid,
                "timestamp": item.get("timestamp"),
                "message": item.get("message"),
                "github_path": item.get("github_path"),
            }
        )
    return evidence


def _normalize_fix(fix: Any) -> dict[str, Any] | None:
    if not isinstance(fix, dict) or not fix.get("status") or not fix.get("base_sha"):
        return None
    return {
        "status": fix.get("status"),
        "base_sha": fix.get("base_sha"),
        "diff": fix.get("diff"),
        "pr_link": fix.get("pr_link"),
    }


def _normalize_recommendations(raw: list[Any] | None) -> list[dict[str, Any]]:
    recommendations = []
    for rec in raw or []:
        if not isinstance(rec, dict):
            continue
        recommendations.append(
            {
                "priority": rec.get("priority"),
                "action": rec.get("action"),
                "github_path": rec.get("github_path"),
                "details": rec.get("details"),
                "evidence_ref": rec.get("evidence_ref"),
                "fix": _normalize_fix(rec.get("fix")),
            }
        )
    return recommendations


def _normalize_causal_chain(raw: list[Any] | None) -> list[dict[str, Any]]:
    causal_chain = []
    for step in raw or []:
        if not isinstance(step, dict):
            continue
        causal_chain.append(
            {
                "step": step.get("step"),
                "relationship": step.get("relationship"),
                "statement": step.get("statement"),
                "evidence_ref": step.get("evidence_ref"),
            }
        )
    return causal_chain


def _normalize_misidentifications(raw: list[Any] | None) -> list[dict[str, Any]]:
    misidentifications = []
    for item in raw or []:
        if not isinstance(item, dict):
            continue
        misidentifications.append(
            {
                "theory": item.get("theory"),
                "why_suspected": item.get("why_suspected"),
                "why_ruled_out": item.get("why_ruled_out"),
                "evidence_ref": item.get("evidence_ref"),
            }
        )
    return misidentifications


def build_root_cause(job: dict[str, Any], jid: str) -> dict[str, Any]:
    """Assemble the root_cause JSONB payload from the job summary and its step5 analysis."""
    step5 = _load_step5(job.get("analysis_path"), jid) or {}
    root_cause = {
        "summary": job.get("root_cause_summary", ""),
        "platform": job.get("platform"),
        "failing_role": job.get("failing_role"),
        "failing_github_path": job.get("failing_github_path"),
        "analysis_path": job.get("analysis_path"),
        "evidence": _normalize_evidence(step5.get("evidence"), jid),
        "causal_chain": _normalize_causal_chain(step5.get("causal_chain")),
        "misidentifications": _normalize_misidentifications(step5.get("misidentifications")),
        "recommendations": _normalize_recommendations(step5.get("recommendations")),
    }
    try:
        jsonschema.validate(instance=root_cause, schema=_ROOT_CAUSE_SCHEMA)
    except jsonschema.ValidationError as e:
        print(f"[WARN] job {jid}: assembled root_cause failed schema validation: {e.message}", file=sys.stderr)
    return root_cause


def find_match(cur: Any, results_table: str, job: dict[str, Any]) -> int | None:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    cutoff_batch_id = f"batch_{cutoff.strftime('%Y%m%d_%H%M%S')}"

    cur.execute(
        psycopg2.sql.SQL(
            """SELECT id, root_cause->>'summary' AS root_cause_summary FROM {}
               WHERE root_cause_category = %s AND catalog_item = %s
                 AND confidence = 'high' AND batch_id >= %s
                 AND {active}"""
        ).format(
            psycopg2.sql.Identifier(results_table),
            active=known_issue_active_sql(cur.connection, table=results_table),
        ),
        (job.get("root_cause_category"), job.get("catalog_item"), cutoff_batch_id),
    )
    summary = job.get("root_cause_summary", "")
    for row in cur.fetchall():
        if isinstance(row, Mapping):
            row_id = row["id"]
            existing_summary = row["root_cause_summary"]
        else:
            row_id, existing_summary = row
        ratio = difflib.SequenceMatcher(None, summary, existing_summary or "").ratio()
        if ratio >= MATCH_THRESHOLD:
            print(f"[MATCH] job {job.get('job_id')} ({ratio:.0%}) -> result {row_id}")
            print(f"  Current:    {summary[:120]}")
            print(f"  Historical: {(existing_summary or '')[:120]}")
            return row_id
    return None


def _validate_match_id(cur: Any, results_table: str, matched_id: int) -> bool:
    """Confirm the agent's cited match exists and is a high-confidence result."""
    cur.execute(
        psycopg2.sql.SQL("SELECT 1 FROM {} WHERE id = %s AND confidence = 'high'").format(
            psycopg2.sql.Identifier(results_table)
        ),
        (matched_id,),
    )
    return cur.fetchone() is not None


def store_report(
    conn: Any, config: dict[str, Any], report: dict[str, Any], filename: str | None = None
) -> bool:
    batch_id = report.get("batch_id")
    if not batch_id and filename:
        base = os.path.splitext(os.path.basename(filename))[0]
        batch_id = base
    if not batch_id:
        print("[ERROR] Cannot determine batch_id", file=sys.stderr)
        return False

    results_table = config["results_table"]
    source_table = config["source_table"]

    jobs = report.get("job_results") or report.get("job_summaries") or report.get("jobs", [])
    result_ids_by_job: dict[str, int] = {}
    with conn.cursor() as cur:
        for job in jobs:
            jid = str(job.get("job_id", ""))
            status = job.get("status")

            matched_id = None
            if status in ("analyzed", "matched_known_issue"):
                candidate_id = job.get("matched_result_id")
                if candidate_id is None:
                    hist = job.get("historical_matches")
                    if hist and isinstance(hist, list) and len(hist) > 0:
                        candidate_id = hist[0].get("matched_result_id")

                if candidate_id is not None:
                    if _validate_match_id(cur, results_table, candidate_id):
                        matched_id = candidate_id
                        print(f"[MATCH-AGENT] job {jid} -> result {matched_id} (validated)")
                    else:
                        print(f"[WARN] job {jid}: declared match {candidate_id} failed validation")

                if matched_id is None:
                    matched_id = find_match(cur, results_table, job)

            if matched_id is not None:
                result_ids_by_job[jid] = matched_id
                job["result_id"] = matched_id
                cur.execute(
                    psycopg2.sql.SQL(
                        """UPDATE {} SET aap2_job_results_fk_id = %s, ai_processed = TRUE
                           WHERE job_id = %s"""
                    ).format(psycopg2.sql.Identifier(source_table)),
                    (matched_id, jid),
                )
            else:
                cur.execute(
                    psycopg2.sql.SQL(
                        """INSERT INTO {}
                           (batch_id, job_id, status, root_cause_category, root_cause,
                            confidence, catalog_item, job_duration_seconds)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (batch_id, job_id) DO UPDATE SET status = EXCLUDED.status
                           RETURNING id"""
                    ).format(psycopg2.sql.Identifier(results_table)),
                    (
                        batch_id,
                        jid,
                        job.get("status"),
                        job.get("root_cause_category"),
                        psycopg2.extras.Json(build_root_cause(job, jid)),
                        job.get("confidence"),
                        job.get("catalog_item"),
                        job.get("job_duration_seconds"),
                    ),
                )
                row = cur.fetchone()
                new_id = (row["id"] if isinstance(row, Mapping) else row[0]) if row else None
                if new_id is not None:
                    result_ids_by_job[jid] = new_id
                    job["result_id"] = new_id

                if status in ("analyzed", "matched_known_issue"):
                    cur.execute(
                        psycopg2.sql.SQL(
                        """UPDATE {} SET aap2_job_results_fk_id = %s, ai_processed = TRUE
                           WHERE job_id = %s"""
                    ).format(psycopg2.sql.Identifier(source_table)),
                    (new_id, jid),
                    )

    for pattern in report.get("cross_job_patterns", []):
        if pattern.get("pattern_id"):
            continue
        result_ids = [
            result_ids_by_job[str(job_id)]
            for job_id in pattern.get("jobs", [])
            if str(job_id) in result_ids_by_job
        ]
        if result_ids:
            pattern["pattern_id"] = str(min(result_ids))

    conn.commit()
    return True


def store_pre_matched(conn: Any, config: dict[str, Any], pre_matched: list[dict[str, Any]]) -> int:
    """Store pre-filtered jobs that were matched before Claude invocation."""
    source_table = config["source_table"]
    count = 0
    with conn.cursor() as cur:
        for job in pre_matched:
            jid = str(job["job_id"])
            matched_id = job["matched_result_id"]
            cur.execute(
                psycopg2.sql.SQL(
                    """UPDATE {} SET aap2_job_results_fk_id = %s, ai_processed = TRUE
                       WHERE job_id = %s"""
                ).format(psycopg2.sql.Identifier(source_table)),
                (matched_id, jid),
            )
            print(
                f"[PRE-MATCH] job {jid} -> result {matched_id}"
                f" (reason: {job.get('match_reason', 'catalog_item')})"
            )
            count += 1
    conn.commit()
    return count


def link_intra_batch_dupes(
    conn: Any, config: dict[str, Any], intra_batch_dupes: list[dict[str, Any]]
) -> int:
    """Copy aap2_job_results_fk_id from representative to each duplicate in the same batch."""
    source_table = config["source_table"]
    count = 0
    with conn.cursor() as cur:
        for entry in intra_batch_dupes:
            dupe_id = str(entry["job_id"])
            rep_id = str(entry["representative_job_id"])

            cur.execute(
                psycopg2.sql.SQL("SELECT aap2_job_results_fk_id FROM {} WHERE job_id = %s").format(
                    psycopg2.sql.Identifier(source_table)
                ),
                (rep_id,),
            )
            row = cur.fetchone()
            fk_id = (
                row.get("aap2_job_results_fk_id") if isinstance(row, Mapping) else row[0]
            ) if row else None
            if fk_id is None:
                print(
                    f"[WARN] intra-batch dupe {dupe_id}: representative {rep_id} has no FK yet",
                    file=sys.stderr,
                )
                continue

            cur.execute(
                psycopg2.sql.SQL(
                    """UPDATE {} SET aap2_job_results_fk_id = %s, ai_processed = TRUE
                       WHERE job_id = %s"""
                ).format(psycopg2.sql.Identifier(source_table)),
                (fk_id, dupe_id),
            )
            print(f"[DUPE-LINK] job {dupe_id} -> result {fk_id} (via representative {rep_id})")
            count += 1

    conn.commit()
    return count


def store_cross_patterns(conn: Any, config: dict[str, Any], report: dict[str, Any]) -> None:
    patterns = report.get("cross_job_patterns", [])
    if not patterns:
        return
    results_table = config["results_table"]
    batch_id = report.get("batch_id", "")
    jobs = report.get("job_results") or report.get("job_summaries") or report.get("jobs", [])
    result_ids_by_job = {
        str(job.get("job_id")): job.get("result_id")
        for job in jobs
        if job.get("result_id") is not None
    }
    with conn.cursor() as cur:
        for p in patterns:
            pattern_id = p.get("pattern_id")
            if not pattern_id:
                continue
            pattern_name = str(pattern_id)
            description = p.get("description")
            for job_id in p.get("jobs", []):
                result_id = result_ids_by_job.get(str(job_id))
                if result_id is not None:
                    cur.execute(
                        psycopg2.sql.SQL(
                            """UPDATE {}
                               SET cross_job_pattern = %s, cross_job_pattern_description = %s
                               WHERE id = %s"""
                        ).format(psycopg2.sql.Identifier(results_table)),
                        (pattern_name, description, result_id),
                    )
                    continue
                cur.execute(
                    psycopg2.sql.SQL(
                        """UPDATE {}
                           SET cross_job_pattern = %s, cross_job_pattern_description = %s
                           WHERE batch_id = %s AND job_id = %s"""
                    ).format(psycopg2.sql.Identifier(results_table)),
                    (pattern_name, description, batch_id, str(job_id)),
                )
        for job in jobs:
            for match in job.get("historical_matches", []) or []:
                pattern_id = match.get("pattern_id")
                matched_result_id = match.get("matched_result_id")
                if not pattern_id or matched_result_id is None:
                    continue
                cur.execute(
                    psycopg2.sql.SQL(
                        """UPDATE {}
                           SET cross_job_pattern = %s
                           WHERE id = %s AND cross_job_pattern IS NULL"""
                    ).format(psycopg2.sql.Identifier(results_table)),
                    (str(pattern_id), matched_result_id),
                )
    conn.commit()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Store batch RCA reports in PostgreSQL")
    parser.add_argument("report", nargs="?", help="Path to a batch report JSON file")
    parser.add_argument("--backfill", metavar="DIR", help="Load all batch_*.json files from DIR")
    parser.add_argument(
        "--pre-matched",
        metavar="JSON",
        help="JSON array of pre-matched jobs from pre_filter_jobs.py (stdin if '-')",
    )
    parser.add_argument(
        "--link-dupes",
        metavar="JSON",
        help="JSON array of intra-batch dupes [{job_id, representative_job_id}] to link",
    )
    args = parser.parse_args(argv)

    if not args.report and not args.backfill and not args.pre_matched and not args.link_dupes:
        parser.error(
            "Provide a report path, --backfill DIR, --pre-matched JSON, or --link-dupes JSON"
        )

    try:
        config = load_database_config(required=("name", "user", "password", "results_table", "source_table"))
    except SystemExit:
        return 1

    try:
        conn = connect_db(config)
    except psycopg2.OperationalError as e:
        print(f"Cannot connect to database: {e}", file=sys.stderr)
        return 1

    print(f"[INFO] Connected as user: {config['user']} on {config['results_table']}")

    if args.pre_matched:
        try:
            if args.pre_matched == "-":
                pre_matched = json.load(sys.stdin)
            else:
                pre_matched = json.loads(args.pre_matched)
            count = store_pre_matched(conn, config, pre_matched)
            print(f"[DONE] {count} pre-matched job(s) stored")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[ERROR] Invalid pre-matched JSON: {e}", file=sys.stderr)
            return 1
        finally:
            conn.close()
        return 0

    if args.link_dupes:
        try:
            dupes = json.loads(args.link_dupes)
            count = link_intra_batch_dupes(conn, config, dupes)
            print(f"[DONE] {count} intra-batch dupe(s) linked")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[ERROR] Invalid link-dupes JSON: {e}", file=sys.stderr)
            return 1
        finally:
            conn.close()
        return 0

    files: list[str] = []
    if args.backfill:
        files = sorted(glob.glob(os.path.join(args.backfill, "batch_*.json")))
        if not files:
            print(f"[WARN] No batch_*.json files found in {args.backfill}", file=sys.stderr)
            conn.close()
            return 0
    elif args.report:
        files = [args.report]

    inserted = 0
    for path in files:
        try:
            with open(path) as f:
                report = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"[ERROR] Failed to read {path}: {e}", file=sys.stderr)
            continue

        if store_report(conn, config, report, filename=path):
            bid = report.get("batch_id") or os.path.splitext(os.path.basename(path))[0]
            jobs = (
                report.get("job_results") or report.get("job_summaries") or report.get("jobs", [])
            )
            inserted += 1
            print(f"[OK] Stored {bid} ({len(jobs)} jobs)")
            try:
                store_cross_patterns(conn, config, report)
            except Exception as e:
                print(f"[WARN] Failed to store cross_job_patterns: {e}", file=sys.stderr)

    conn.close()
    print(f"[DONE] {inserted}/{len(files)} report(s) stored in {config['results_table']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
