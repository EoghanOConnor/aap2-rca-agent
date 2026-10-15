"""Store batch RCA JSON reports into a results table on the source database."""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections.abc import Mapping
from importlib.resources import files
from typing import Any

import jsonschema
import psycopg2
import psycopg2.extras
import psycopg2.sql

from rca.config import load_database_config
from rca.database import connect_db
from rca.batch.match import Confidence, MatchSignals, score_similarity, weakest_confidence



_ROOT_CAUSE_SCHEMA: dict[str, Any] = json.loads(
    files("rca")
    .joinpath("schemas")
    .joinpath("root_cause.schema.json")
    .read_text(encoding="utf-8")
)


def _load_step5(analysis_path: str | None, jid: str) -> dict[str, Any] | None:
    """Read step5_analysis_summary.json from the job's analysis directory, if available."""
    if not analysis_path:
        return None
    path = os.path.join(analysis_path, "step5_analysis_summary.json")
    try:
        with open(path) as stream:
            return json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[WARN] job {jid}: could not read step5 analysis ({path}): {exc}", file=sys.stderr)
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
    except jsonschema.ValidationError as exc:
        print(
            f"[WARN] job {jid}: assembled root_cause failed schema validation: {exc.message}",
            file=sys.stderr,
        )
    return root_cause


def store_report(
    conn: Any,
    config: dict[str, Any],
    report: dict[str, Any],
    filename: str | None = None,
    *,
    commit: bool = True,
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
            cur.execute(
                psycopg2.sql.SQL(
                    """INSERT INTO {}
                       (batch_id, job_id, status, root_cause_category, root_cause,
                        confidence, catalog_item, job_duration_seconds)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (batch_id, job_id) DO UPDATE SET
                           status = EXCLUDED.status,
                           root_cause_category = EXCLUDED.root_cause_category,
                           root_cause = EXCLUDED.root_cause,
                           confidence = EXCLUDED.confidence,
                           catalog_item = EXCLUDED.catalog_item,
                           job_duration_seconds = EXCLUDED.job_duration_seconds
                       RETURNING id"""
                ).format(psycopg2.sql.Identifier(results_table)),
                (
                    batch_id,
                    jid,
                    status,
                    job.get("root_cause_category"),
                    psycopg2.extras.Json(build_root_cause(job, jid)),
                    job.get("confidence"),
                    job.get("catalog_item"),
                    job.get("job_duration_seconds"),
                ),
            )
            row = cur.fetchone()
            new_id = (row["id"] if isinstance(row, Mapping) else row[0]) if row else None
            if new_id is None:
                print(f"[ERROR] Result insert for job {jid} did not return an ID", file=sys.stderr)
                conn.rollback()
                return False

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

    if commit:
        conn.commit()
    return True


def store_pre_matched(
    conn: Any,
    config: dict[str, Any],
    pre_matched: list[dict[str, Any]],
    *,
    commit: bool = True,
) -> int:
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
    if commit:
        conn.commit()
    return count


def link_intra_batch_dupes(
    conn: Any,
    config: dict[str, Any],
    intra_batch_dupes: list[dict[str, Any]],
    *,
    commit: bool = True,
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

    if commit:
        conn.commit()
    return count


def _job_summary_signals(job: dict[str, Any]) -> MatchSignals:
    return MatchSignals(
        catalog_item=job.get("catalog_item"),
        text=job.get("root_cause_summary"),
        category=job.get("root_cause_category"),
    )


def _row_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return {
        "id": row[0],
        "catalog_item": row[1],
        "root_cause_category": row[2],
        "root_cause_summary": row[3],
        "related_result_id": row[4],
        "related_result_confidence": row[5],
    }


def _fetch_results(
    cur: Any, results_table: str, result_ids: list[int]
) -> dict[int, dict[str, Any]]:
    unique_ids = sorted({int(result_id) for result_id in result_ids})
    if not unique_ids:
        return {}

    cur.execute(
        psycopg2.sql.SQL(
            """SELECT id, catalog_item, root_cause_category,
                      root_cause->>'summary' AS root_cause_summary,
                      related_result_id, related_result_confidence
               FROM {} WHERE id = ANY(%s)
               FOR UPDATE"""
        ).format(psycopg2.sql.Identifier(results_table)),
        (unique_ids,),
    )
    rows = [_row_dict(row) for row in cur.fetchall()]
    return {int(row["id"]): row for row in rows}


def _resolved_anchors(
    cur: Any,
    results_table: str,
    rows: list[dict[str, Any]],
    known_rows: dict[int, dict[str, Any]] | None = None,
) -> dict[int, int]:
    states: dict[int, dict[str, Any]] = {
        int(row["id"]): {
            "anchor": int(row["id"]),
            "candidate": row.get("related_result_id"),
            "seen": {int(row["id"])},
        }
        for row in rows
    }
    row_cache = dict(known_rows or {})

    while True:
        candidate_ids = sorted(
            {
                int(state["candidate"])
                for state in states.values()
                if state["candidate"] is not None
            }
        )
        if not candidate_ids:
            break

        missing_ids = [result_id for result_id in candidate_ids if result_id not in row_cache]
        row_cache.update(_fetch_results(cur, results_table, missing_ids))
        advanced = False
        for state in states.values():
            candidate = state["candidate"]
            if candidate is None:
                continue
            candidate_id = int(candidate)
            anchor_row = row_cache.get(candidate_id)
            if anchor_row is None or candidate_id in state["seen"]:
                state["candidate"] = None
                advanced = True
                continue

            anchor_id = int(anchor_row["id"])
            state["anchor"] = anchor_id
            state["candidate"] = anchor_row.get("related_result_id")
            state["seen"].add(anchor_id)
            advanced = True

        if not advanced:
            break

    return {
        result_id: int(state["anchor"]) for result_id, state in states.items()
    }


def _assign_clusters(
    cur: Any,
    results_table: str,
    links: list[tuple[int, int, Confidence]],
    known_rows: dict[int, dict[str, Any]] | None = None,
) -> list[int | None]:
    if not links:
        return []

    lookup_ids = {
        int(result_id)
        for source_result_id, target_result_id, _ in links
        for result_id in (source_result_id, target_result_id)
    }
    existing_rows = dict(known_rows or {})
    missing_ids = sorted(lookup_ids - set(existing_rows))
    existing_rows.update(_fetch_results(cur, results_table, missing_ids))
    resolved_anchors = _resolved_anchors(
        cur, results_table, list(existing_rows.values()), known_rows=existing_rows
    )

    parent: dict[int, int] = {}
    confidences: dict[int, Confidence] = {}

    def find(result_id: int) -> int:
        parent.setdefault(result_id, result_id)
        root = result_id
        while parent[root] != root:
            root = parent[root]
        while parent[result_id] != root:
            parent[result_id], result_id = root, parent[result_id]
        return root

    def merge_confidence(anchor: int, confidence: Confidence) -> None:
        root = find(anchor)
        current = confidences.get(root)
        confidences[root] = (
            confidence
            if current is None
            else weakest_confidence([current, confidence]) or current
        )

    assigned_anchors: list[int | None] = [None] * len(links)
    valid_link_indices: set[int] = set()
    for index, (source_result_id, target_result_id, confidence) in enumerate(links):
        if source_result_id not in existing_rows or target_result_id not in existing_rows:
            continue

        valid_link_indices.add(index)
        source_root = find(resolved_anchors[source_result_id])
        target_root = find(resolved_anchors[target_result_id])
        if source_root != target_root:
            source_confidence = confidences.pop(source_root, None)
            target_confidence = confidences.pop(target_root, None)
            canonical_root = min(source_root, target_root)
            parent[source_root] = canonical_root
            parent[target_root] = canonical_root
            parent[canonical_root] = canonical_root
            for existing_confidence in (source_confidence, target_confidence):
                if existing_confidence is not None:
                    merge_confidence(canonical_root, existing_confidence)

        merge_confidence(source_root, confidence)
        assigned_anchors[index] = source_root

    if not valid_link_indices:
        return assigned_anchors

    existing_anchors = {
        resolved_anchors[result_id]
        for index in valid_link_indices
        for result_id in links[index][:2]
    }
    cur.execute(
        psycopg2.sql.SQL(
            """SELECT id, related_result_id, related_result_confidence
               FROM {}
               WHERE id = ANY(%s) OR related_result_id = ANY(%s)
               FOR UPDATE"""
        ).format(psycopg2.sql.Identifier(results_table)),
        (
            sorted(existing_anchors),
            sorted(existing_anchors),
        ),
    )
    for row in cur.fetchall():
        row_id = int(row["id"] if isinstance(row, Mapping) else row[0])
        row_anchor = row.get("related_result_id") if isinstance(row, Mapping) else row[1]
        row_confidence = (
            row.get("related_result_confidence")
            if isinstance(row, Mapping)
            else row[2]
        )
        if row_confidence in ("high", "medium", "low"):
            merge_confidence(int(row_anchor or row_id), row_confidence)

    old_anchors = sorted(existing_anchors)
    canonical_anchors = [find(anchor) for anchor in old_anchors]
    confidence_values = [
        confidences.get(find(anchor), "low") for anchor in old_anchors
    ]

    cur.execute(
        psycopg2.sql.SQL(
            """UPDATE {} AS result
               SET related_result_id =
                       CASE WHEN result.id = cluster.canonical
                            THEN NULL ELSE cluster.canonical END,
                   related_result_confidence =
                       CASE WHEN result.id = cluster.canonical
                            THEN NULL ELSE cluster.confidence END
               FROM unnest(%s::int[], %s::int[], %s::text[])
                   AS cluster(old_anchor, canonical, confidence)
               WHERE result.related_result_id = cluster.old_anchor
                  OR result.id = cluster.old_anchor"""
        ).format(psycopg2.sql.Identifier(results_table)),
        (
            old_anchors,
            canonical_anchors,
            confidence_values,
        ),
    )
    for index, (source_result_id, _, _) in enumerate(links):
        if index in valid_link_indices:
            assigned_anchors[index] = find(resolved_anchors[source_result_id])
    return assigned_anchors


def post_analysis_link(
    conn: Any,
    config: dict[str, Any],
    report: dict[str, Any],
    *,
    commit: bool = True,
) -> None:
    """Apply validated semantic and deterministic links as canonical clusters."""

    results_table = config["results_table"]
    source_table = config["source_table"]
    jobs = report.get("job_results") or report.get("job_summaries") or report.get("jobs", [])
    jobs_by_id = {str(job.get("job_id")): job for job in jobs}
    job_index_by_id = {str(job.get("job_id")): index for index, job in enumerate(jobs)}
    result_ids_by_job = {
        job_id: job.get("result_id")
        for job_id, job in jobs_by_id.items()
        if job.get("result_id") is not None
    }

    with conn.cursor() as cur:
        known_rows: dict[int, dict[str, Any]] = {}
        links: list[tuple[int, int, Confidence]] = []
        pattern_link_ranges: dict[int, tuple[int, int]] = {}
        pattern_job_indices: dict[int, list[int]] = {}
        patterns = report.get("cross_job_patterns", []) or []
        for pattern_index, pattern in enumerate(patterns):
            # Clear any fallback pattern_id store_report() stamped before this
            # pattern is validated; it's only restored below once a cluster is
            # actually confirmed and persisted, so a rejected pattern can't
            # leave the report claiming a link the DB never got.
            pattern.pop("pattern_id", None)
            pattern_jobs = [
                jobs_by_id[str(job_id)]
                for job_id in pattern.get("jobs", [])
                if str(job_id) in jobs_by_id
            ]
            if len(pattern_jobs) < 2:
                continue
            pairwise_confidences: list[Confidence | None] = []
            for left_index, left_job in enumerate(pattern_jobs):
                for right_job in pattern_jobs[left_index + 1 :]:
                    score = score_similarity(
                        _job_summary_signals(left_job),
                        _job_summary_signals(right_job),
                        "post_analysis",
                        semantic_confidence=pattern.get("confidence"),
                        semantic_reasoning=pattern.get("description"),
                    )
                    pairwise_confidences.append(score.confidence)
            confidence = weakest_confidence(pairwise_confidences)
            if confidence is None:
                continue

            pattern_result_ids = [
                int(result_ids_by_job[str(job["job_id"])])
                for job in pattern_jobs
                if str(job["job_id"]) in result_ids_by_job
            ]
            if len(pattern_result_ids) != len(pattern_jobs):
                continue
            pattern_member_job_indices = [
                job_index_by_id[str(job["job_id"])]
                for job in pattern_jobs
                if str(job["job_id"]) in job_index_by_id
            ]

            # Every member's row must exist, or the whole pattern is dropped —
            # a partial merge would silently leave one job out of its own cluster.
            missing_ids = [
                result_id for result_id in pattern_result_ids if result_id not in known_rows
            ]
            if missing_ids:
                known_rows.update(_fetch_results(cur, results_table, missing_ids))
            if any(result_id not in known_rows for result_id in pattern_result_ids):
                continue

            anchor = min(pattern_result_ids)
            link_start = len(links)
            links.extend(
                (result_id, anchor, confidence) for result_id in pattern_result_ids
            )
            pattern_link_ranges[pattern_index] = (link_start, len(links))
            pattern_job_indices[pattern_index] = pattern_member_job_indices

        historical_target_ids = sorted(
            {
                int(match["matched_result_id"])
                for job in jobs
                for match in job.get("historical_matches", []) or []
                if match.get("matched_result_id") is not None
            }
        )
        missing_historical_ids = [
            result_id for result_id in historical_target_ids if result_id not in known_rows
        ]
        if missing_historical_ids:
            known_rows.update(_fetch_results(cur, results_table, missing_historical_ids))
        historical_rows = known_rows
        historical_link_indices: dict[tuple[int, int], int] = {}
        for job_index, job in enumerate(jobs):
            for match_index, match in enumerate(job.get("historical_matches", []) or []):
                matched_result_id = match.get("matched_result_id")
                if matched_result_id is None:
                    continue
                target_row = historical_rows.get(int(matched_result_id))
                if target_row is None:
                    continue
                # match_source is set only by orchestrator._deterministic_historical_matches;
                # the LLM's structured output never carries this key (_normalize_semantics
                # builds its match dicts from a fixed set of keys that excludes it), so this
                # can't be spoofed by free-text evidence fields the agent controls.
                is_deterministic_fallback = match.get("match_source") == "deterministic_fallback"
                score = score_similarity(
                    _job_summary_signals(job),
                    MatchSignals(
                        catalog_item=target_row.get("catalog_item"),
                        text=target_row.get("root_cause_summary"),
                        category=target_row.get("root_cause_category"),
                    ),
                    "post_analysis",
                    semantic_confidence=(
                        None if is_deterministic_fallback else match.get("confidence")
                    ),
                    semantic_reasoning=(
                        None if is_deterministic_fallback else match.get("similarity_reasoning")
                    ),
                )
                if score.confidence is None:
                    continue
                result_id = job.get("result_id")
                if result_id is None:
                    continue
                historical_link_indices[(job_index, match_index)] = len(links)
                links.append(
                    (int(result_id), int(matched_result_id), score.confidence)
                )

        canonical_anchors = _assign_clusters(
            cur,
            results_table,
            links,
            known_rows=known_rows,
        )
        source_match_result_ids: dict[int, int] = {}

        def register_source_anchor(job_index: int, anchor: int) -> None:
            current_anchor = source_match_result_ids.get(job_index)
            source_match_result_ids[job_index] = (
                anchor if current_anchor is None else min(current_anchor, anchor)
            )

        for pattern_index, (link_start, link_end) in pattern_link_ranges.items():
            pattern = report["cross_job_patterns"][pattern_index]
            assigned = canonical_anchors[link_start:link_end]
            if not assigned or any(anchor is None for anchor in assigned):
                continue
            pattern_anchor = min(
                anchor for anchor in assigned if anchor is not None
            )
            pattern["pattern_id"] = str(pattern_anchor)
            # Jobs linked only through a same-batch pattern (no historical
            # match) still need their source-table FK repointed to the
            # cluster anchor, the same as a historical-matched job gets below.
            for job_index, canonical_anchor in zip(
                pattern_job_indices[pattern_index], assigned
            ):
                if canonical_anchor is not None:
                    register_source_anchor(job_index, canonical_anchor)

        for (job_index, match_index), link_index in historical_link_indices.items():
            canonical_anchor = canonical_anchors[link_index]
            if canonical_anchor is None:
                continue
            job = jobs[job_index]
            match = job["historical_matches"][match_index]
            match["pattern_id"] = str(canonical_anchor)
            register_source_anchor(job_index, canonical_anchor)

        for job_index, source_match_result_id in source_match_result_ids.items():
            job_id = str(jobs[job_index].get("job_id", ""))
            if not job_id:
                continue
            cur.execute(
                psycopg2.sql.SQL(
                    """UPDATE {}
                       SET aap2_job_results_fk_id = %s, ai_processed = TRUE
                       WHERE job_id = %s"""
                ).format(psycopg2.sql.Identifier(source_table)),
                (source_match_result_id, job_id),
            )

    if commit:
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

        if store_report(conn, config, report, filename=path, commit=False):
            bid = report.get("batch_id") or os.path.splitext(os.path.basename(path))[0]
            jobs = (
                report.get("job_results") or report.get("job_summaries") or report.get("jobs", [])
            )
            inserted += 1
            print(f"[OK] Stored {bid} ({len(jobs)} jobs)")
            try:
                post_analysis_link(conn, config, report)
            except Exception as e:
                conn.rollback()
                inserted -= 1
                print(f"[WARN] Failed to apply post-analysis links: {e}", file=sys.stderr)

    conn.close()
    print(f"[DONE] {inserted}/{len(files)} report(s) stored in {config['results_table']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
