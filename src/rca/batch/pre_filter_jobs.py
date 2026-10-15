"""Unified pre-analysis gate for duplicate and known-issue matching."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
import psycopg2.sql

from rca.config import load_database_config
from rca.database import connect_db, known_issue_active_sql
from rca.batch.match import MatchSignals, score_similarity



def extract_catalog_item(job_name: str) -> str | None:
    """Extract catalog_item from job_name.

    Handles the RHPDS format: 'RHPDS {platform}.{catalog_item}.{env}-{guid}-{action} ...'
    """
    name = job_name.removeprefix("RHPDS ").strip()
    if " " in name:
        name = name.split(maxsplit=1)[0]
    parts = name.split(".")
    if len(parts) >= 3:
        return ".".join(parts[1:-1])
    if len(parts) == 2:
        return parts[1].split("-")[0] if "-" in parts[1] else parts[1]
    return None


def fetch_recent_results(
    conn: Any,
    results_table: str,
    source_table: str,
    lookback_hours: int = 4,
) -> list[dict[str, Any]]:
    """Fetch active, recent, high-confidence results for pre-analysis matching."""

    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    cutoff_batch_id = f"batch_{cutoff.strftime('%Y%m%d_%H%M%S')}"

    with conn.cursor() as cur:
        cur.execute(
            psycopg2.sql.SQL(
                """SELECT r.id, r.catalog_item, r.root_cause_category,
                          r.root_cause->>'summary' AS root_cause_summary, e.error_message
                   FROM {results} r
                   LEFT JOIN {source} e ON r.job_id::text = e.job_id::text
                   WHERE r.confidence = 'high'
                     AND r.batch_id >= %s
                     AND r.status = 'analyzed'
                     AND {active}
                   ORDER BY r.batch_id DESC, r.id DESC"""
            ).format(
                results=psycopg2.sql.Identifier(results_table),
                source=psycopg2.sql.Identifier(source_table),
                active=known_issue_active_sql(conn, alias="r", table=results_table),
            ),
            (cutoff_batch_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def _job_signal(meta: dict[str, Any] | None) -> MatchSignals | None:
    if not meta:
        return None
    return MatchSignals(
        catalog_item=extract_catalog_item(meta.get("job_name", "")),
        text=meta.get("error_message"),
    )


def _historical_signal(result: dict[str, Any]) -> MatchSignals:
    return MatchSignals(
        catalog_item=result.get("catalog_item"),
        text=result.get("error_message"),
        category=result.get("root_cause_category"),
    )


def pre_analysis_gate(
    job_ids: list[int],
    job_metadata: dict[int, dict[str, Any]],
    historical: list[dict[str, Any]],
    *,
    use_history: bool = True,
) -> tuple[list[int], dict[int, int], list[dict[str, Any]]]:
    """Cluster incoming jobs and conservatively skip known issues."""

    ordered_job_ids = list(dict.fromkeys(job_ids))
    clusters: list[list[int]] = []
    for job_id in sorted(ordered_job_ids):
        signal = _job_signal(job_metadata.get(job_id))
        if signal is None:
            clusters.append([job_id])
            continue

        for cluster in clusters:
            representative_signal = _job_signal(job_metadata.get(cluster[0]))
            if representative_signal is None:
                continue
            result = score_similarity(signal, representative_signal, "pre_analysis")
            if result.confidence == "high":
                cluster.append(job_id)
                break
        else:
            clusters.append([job_id])

    grouped_by_catalog: dict[str, list[dict[str, Any]]] = {}
    for result in historical:
        catalog_item = result.get("catalog_item")
        if catalog_item:
            grouped_by_catalog.setdefault(catalog_item, []).append(result)
    ambiguous_catalogs = {
        catalog_item
        for catalog_item, results in grouped_by_catalog.items()
        if len({result.get("root_cause_category") for result in results}) > 1
    }

    skip_targets: dict[int, int] = {}
    analyze_ids: list[int] = []
    for cluster in clusters:
        representative = cluster[0]
        if not use_history:
            analyze_ids.append(representative)
            continue

        representative_signal = _job_signal(job_metadata.get(representative))
        best_result: dict[str, Any] | None = None
        best_score = 0.0
        for historical_result in historical:
            if historical_result.get("catalog_item") in ambiguous_catalogs:
                continue
            if representative_signal is None:
                continue
            result = score_similarity(
                representative_signal,
                _historical_signal(historical_result),
                "pre_analysis",
            )
            if result.confidence != "high" or result.score < best_score:
                continue
            if (
                best_result is not None
                and result.score == best_score
                and int(historical_result["id"]) >= int(best_result["id"])
            ):
                continue
            best_result = historical_result
            best_score = result.score

        if best_result is None:
            analyze_ids.append(representative)
        else:
            skip_targets[representative] = int(best_result["id"])

    analyze_set = set(analyze_ids)
    analyze_ids = [job_id for job_id in ordered_job_ids if job_id in analyze_set]
    dupes = [
        {"job_id": job_id, "representative_job_id": cluster[0]}
        for cluster in clusters
        for job_id in cluster[1:]
    ]
    return analyze_ids, skip_targets, dupes


def pre_matched_entries(
    skip_targets: dict[int, int],
    historical: list[dict[str, Any]],
    job_ids: list[int],
) -> list[dict[str, Any]]:
    """Format high-confidence pre-analysis skips for persistence and reports."""

    historical_by_id = {int(result["id"]): result for result in historical}
    original_order = {job_id: index for index, job_id in enumerate(job_ids)}
    pre_matched = []
    for job_id, matched_result_id in skip_targets.items():
        result = historical_by_id[matched_result_id]
        pre_matched.append(
            {
                "job_id": job_id,
                "matched_result_id": matched_result_id,
                "catalog_item": result.get("catalog_item"),
                "root_cause_category": result.get("root_cause_category"),
                "match_reason": "pre_analysis_gate",
                "recent_result_summary": str(result.get("root_cause_summary") or "")[:200],
            }
        )
    pre_matched.sort(key=lambda match: original_order.get(match["job_id"], len(job_ids)))
    return pre_matched


def fetch_job_metadata(
    conn: Any, source_table: str, job_ids: list[int]
) -> dict[int, dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            psycopg2.sql.SQL(
                "SELECT job_id, job_name, error_message FROM {} WHERE job_id = ANY(%s)"
            ).format(psycopg2.sql.Identifier(source_table)),
            (job_ids,),
        )
        return {
            row["job_id"]: {
                "job_name": row["job_name"],
                "error_message": row["error_message"],
            }
            for row in cur.fetchall()
            if row["job_name"]
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pre-filter jobs against known results")
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="File with newline-separated job IDs (default: stdin)",
    )
    parser.add_argument(
        "--lookback-hours", type=int, default=4, help="Lookback window (default: 4)"
    )
    parser.add_argument(
        "--dedup-only",
        action="store_true",
        help="Only run intra-batch dedup (no pre-filter against known issues)",
    )
    args = parser.parse_args(argv)

    if args.input:
        with open(args.input) as f:
            raw = f.read()
    else:
        raw = sys.stdin.read()

    job_ids = [int(line.strip()) for line in raw.strip().splitlines() if line.strip()]
    if not job_ids:
        if args.dedup_only:
            print(json.dumps({"representatives": job_ids, "dupes": []}))
        else:
            print(json.dumps({"analyze": [], "pre_matched": []}))
        return 0

    required_keys = ("name", "user", "password", "source_table")
    if not args.dedup_only:
        required_keys = (*required_keys, "results_table")

    try:
        config = load_database_config(required=required_keys)
    except SystemExit:
        return 1

    try:
        conn = connect_db(config, use_dict_cursor=True)
    except psycopg2.OperationalError as e:
        print(f"Cannot connect to database: {e}", file=sys.stderr)
        return 1

    try:
        if args.dedup_only:
            job_metadata = fetch_job_metadata(conn, config["source_table"], job_ids)
            representatives, _skip_targets, dupes = pre_analysis_gate(
                job_ids,
                job_metadata,
                [],
                use_history=False,
            )
            result = {"representatives": representatives, "dupes": dupes}
        else:
            recent_results = fetch_recent_results(
                conn,
                config["results_table"],
                config["source_table"],
                args.lookback_hours,
            )
            job_metadata = fetch_job_metadata(
                conn,
                config["source_table"],
                job_ids,
            )
            analyze_ids, skip_targets, dupes = pre_analysis_gate(
                job_ids,
                job_metadata,
                recent_results,
            )
            pre_matched = pre_matched_entries(
                skip_targets,
                recent_results,
                job_ids,
            )
            result = {"analyze": analyze_ids, "pre_matched": pre_matched, "dupes": dupes}

    except psycopg2.Error as e:
        print(f"Query failed: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
