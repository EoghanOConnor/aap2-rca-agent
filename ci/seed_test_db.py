#!/usr/bin/env python3
"""Generate synthetic AAP job logs and seed the CI test database.

Writes job_{id}.json.gz files into JOB_LOGS_DIR (default: ci/synthetic-logs/
next to this script), matching the ETL-extracted document shape that
rca.analysis.job_parser.parse_job_log expects, then recreates the
source/results tables and inserts one event row per job so the database and
JOB_LOGS_DIR tell the same story. Timestamps are relative to now so --since
windows work on every run.

Database settings come from the same places rca-batch reads them:
RCA_SETTINGS_FILE (settings.json env block) and the process environment,
falling back to the local docker-compose defaults. Because discovery may
pick up settings pointing at a real database, the destructive seeding
requires RCA_CI_SEED_CONFIRM=1 to run.

Splunk, GitHub, and Jira are intentionally left unconfigured: the batch's
steps 2 and 4 record a skip and continue.

The five jobs are designed around the batch's similarity thresholds:
  900001/900002 - same catalog item, near-identical errors  -> dedup/pre-filter cluster
  900003        - same catalog item, different error        -> below 0.75 threshold
  900004        - other catalog item, error ~= 900003       -> cross-catalog (>= 0.90)
  900005        - other catalog item, unique error          -> standalone analysis
"""

from __future__ import annotations

import gzip
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2

DEFAULT_LOGS_DIR = Path(__file__).resolve().parent / "synthetic-logs"

JOBS = [
    {
        "job_id": 900001,
        "job_name": "RHPDS zt-hp.zt-ocp4-get-started-cluster.dev-syn01-1-provision",
        "guid": "syn01-1",
        "cluster": "synthetic-cluster-a",
        "template_id": 203390,
        "duration": 1570,
        "failed_task": {
            "task": "Run security validation",
            "play": "Step 004 Software",
            "role": "ocp_workload_security_validation",
            "task_action": "rhpds.security.validate_tenant_security",
            "msg": (
                "Task failed: Finalization of task args for 'rhpds.security.validate_tenant_security' "
                "failed: 'cluster_admin_agnosticd_sa_token' is undefined"
            ),
        },
    },
    {
        "job_id": 900002,
        "job_name": "RHPDS zt-hp.zt-ocp4-get-started-cluster.dev-syn02-1-provision",
        "guid": "syn02-1",
        "cluster": "synthetic-cluster-a",
        "template_id": 203390,
        "duration": 1521,
        "failed_task": {
            "task": "Run security validation",
            "play": "Step 004 Software",
            "role": "ocp_workload_security_validation",
            "task_action": "rhpds.security.validate_tenant_security",
            "msg": (
                "Task failed: Finalization of task args for 'rhpds.security.validate_tenant_security' "
                "failed: 'cluster_admin_agnosticd_sa_token' is undefined"
            ),
        },
    },
    {
        "job_id": 900003,
        "job_name": "RHPDS zt-hp.zt-ocp4-get-started-cluster.dev-syn03-1-provision",
        "guid": "syn03-1",
        "cluster": "synthetic-cluster-a",
        "template_id": 203390,
        "duration": 2311,
        "failed_task": {
            "task": "Wait for router rollout to complete (IngressController available)",
            "play": "Step 004 Software",
            "role": "ocp4_workload_ingress",
            "task_action": "kubernetes.core.k8s_info",
            "msg": (
                "Platform error: the IngressController operator reported Degraded=True - "
                "ingress controller router-default failed to rollout within the timeout window"
            ),
        },
    },
    {
        "job_id": 900004,
        "job_name": "RHPDS zt-hp.zt-ocp4-ansible-automation.dev-syn04-2-provision",
        "guid": "syn04-2",
        "cluster": "synthetic-cluster-b",
        "template_id": 204001,
        "duration": 2087,
        "failed_task": {
            "task": "Wait for ingress to become available",
            "play": "Step 005 Platform",
            "role": "ocp4_workload_ingress",
            "task_action": "kubernetes.core.k8s_info",
            "msg": (
                "Platform error: the IngressController operator reported Degraded=True - "
                "ingress controller router-default failed to rollout within the timeout window"
            ),
        },
    },
    {
        "job_id": 900005,
        "job_name": "RHPDS zt-aws.zt-ocp4-virtualization-cluster.dev-syn05-3-provision",
        "guid": "syn05-3",
        "cluster": "synthetic-cluster-c",
        "template_id": 203410,
        "duration": 344,
        "failed_task": {
            "task": "Create worker node instance",
            "play": "Step 002 Infrastructure",
            "role": "ocp4_workload_aws_nodes",
            "task_action": "amazon.aws.ec2_instance",
            "msg": (
                "Infrastructure error: AWSServiceError - Instance limit exceeded for instance "
                "type m5.xlarge in availability zone us-east-1a (quota: Running On-Demand m5 instances)"
            ),
        },
    },
]

PLAY_NAMES = [
    "Step 0000 Install Galaxy roles and collections",
    "Step 001 Pre Infrastructure",
    "Step 002 Infrastructure",
    "Step 003 Pre Software",
    "Step 004 Software",
    "Step 005 Platform",
]
ROLE_NAMES = [
    "ocp4_workload_gitea_operator",
    "ocp_workload_security_validation",
    "ocp4_workload_ingress",
    "ocp4_workload_aws_nodes",
    "ocp4_workload_monitoring",
]


def _settings() -> dict[str, str]:
    try:
        from rca.config import load_environment

        return load_environment()
    except ImportError:
        import os

        return dict(os.environ)


def _database_settings(env: dict[str, str]) -> dict[str, Any]:
    return {
        "host": env.get("SOURCE_DB_HOST") or "localhost",
        "port": int(env.get("SOURCE_DB_PORT") or "5433"),
        "dbname": env.get("SOURCE_DB_NAME") or "rca_test",
        "user": env.get("SOURCE_DB_USER") or "rca_test",
        "password": env.get("SOURCE_DB_PASSWORD") or "rca_test",
    }


def _job_window(spec: dict, job_index: int, run_date: datetime) -> tuple[datetime, datetime]:
    started = run_date - timedelta(minutes=30 + job_index * 7, seconds=job_index * 13)
    return started, started + timedelta(seconds=spec["duration"])


def _event(idx, kind, created, **extra):
    event = {
        "event_id": 1000 + idx,
        "event": kind,
        "created": created.isoformat(),
        "counter": idx,
        "stdout": "",
        "verbosity": 0,
    }
    event.update(extra)
    return event


def build_document(spec: dict, job_index: int, run_date: datetime) -> dict:
    job_id = spec["job_id"]
    started, finished = _job_window(spec, job_index, run_date)
    namespace = f"sandbox-{spec['guid']}-ocp4"
    ft = spec["failed_task"]

    events = []
    idx = 0
    events.append(_event(idx, "playbook_on_start", started, event_data={"playbook": "./ansible/site.yml"}))
    idx += 1
    for play in PLAY_NAMES:
        events.append(
            _event(idx, "playbook_on_play_start", started + timedelta(seconds=idx), play=play, event_data={"name": play})
        )
        idx += 1
    # Namespace mention in verbose output
    events.append(
        _event(
            idx,
            "verbose",
            started + timedelta(seconds=20),
            stdout=f"TASK [ocp4_workload_namespace : Create project] namespace: {namespace} created",
        )
    )
    idx += 1
    # A couple of pod references
    events.append(
        _event(
            idx,
            "runner_on_ok",
            started + timedelta(seconds=60),
            task="Wait for pods",
            stdout=f"pod/router-default-7d9c8b6f5-x2k9l is Running",
        )
    )
    idx += 1
    events.append(
        _event(
            idx,
            "runner_on_ok",
            started + timedelta(seconds=90),
            task="Wait for pods",
            stdout=f"pod/coordinator-5f7b8c9d1-mn3pq is Running",
        )
    )
    idx += 1
    # Some ordinary successful tasks. Offsets are fractions of the job window
    # (like the retries below) so the terminal failure stays last even for
    # short jobs.
    for n in range(12):
        events.append(
            _event(
                idx,
                "runner_on_ok",
                started + timedelta(seconds=spec["duration"] * (0.30 + 0.40 * n / 11)),
                task=f"Routine setup task {n}",
                play=ft["play"],
                role=ROLE_NAMES[n % len(ROLE_NAMES)],
                event_data={"res": {"changed": True, "msg": "ok"}},
            )
        )
        idx += 1
    # Retries on the failing task
    for n in range(3):
        events.append(
            _event(
                idx,
                "runner_retry",
                started + timedelta(seconds=spec["duration"] * (0.72 + 0.05 * n)),
                task=ft["task"],
                play=ft["play"],
                role=ft["role"],
                event_data={"res": {"msg": "Retrying task, attempt " + str(n + 1), "attempts": n + 1}},
            )
        )
        idx += 1
    # The terminal failure
    events.append(
        _event(
            idx,
            "runner_on_failed",
            finished - timedelta(seconds=5),
            failed=True,
            task=ft["task"],
            play=ft["play"],
            role=ft["role"],
            created_iso=finished.isoformat(),
            event_data={
                "res": {"msg": ft["msg"], "changed": False},
                "task_path": f"/runner/requirements_collections/ansible_collections/rhpds/security/{ft['task_action']}.yml",
                "task_action": ft["task_action"],
                "duration": 0.02,
            },
        )
    )
    idx += 1

    event_types: dict[str, int] = {}
    for e in events:
        event_types[e["event"]] = event_types.get(e["event"], 0) + 1

    document = {
        "job_id": job_id,
        "extraction_timestamp": (finished + timedelta(seconds=92)).isoformat(),
        "extractor_version": "1.0.0",
        "data_sources": {
            "metadata_available": True,
            "events_available": True,
            "retry_analysis_available": True,
        },
        "metadata": {
            "extraction_metadata": {
                "extraction_timestamp": (finished + timedelta(seconds=90)).isoformat(),
                "extractor_version": "1.0",
            },
            "job_metadata": {
                "job_id": job_id,
                "job_name": spec["job_name"],
                "status": "failed",
                "created": started.isoformat(),
                "started": started.isoformat(),
                "finished": finished.isoformat(),
                "duration_seconds": spec["duration"],
                "elapsed": float(spec["duration"]) + 0.2,
                "launch_type": "scheduled",
                "failed": True,
                "job_explanation": "",
                "ansible_version": "",
                "host_status_counts": {"changed": 13, "ok": 15},
                "cloud_provider": "aws",
                "guid": spec["guid"],
                "sandbox_openshift_cluster": spec["cluster"],
                "env_type": "sandbox",
                "action": "provision",
            },
            "template_metadata": {
                "template_id": spec["template_id"],
                "template_name": spec["job_name"],
                "template_description": "",
                "organization_id": 2,
            },
            "git_context": {
                "scm_url": None,
                "scm_branch": None,
                "scm_revision": None,
                "scm_type": None,
                "execution_environment_image": None,
                "scm_ref": None,
            },
        },
        "events_summary": {
            "total_events": len(events),
            "failed_events": 1,
            "changed_events": 13,
            "event_types": event_types,
        },
        "events": events,
        "retry_analysis": {
            "job_analysis": {
                "total_plays": len(PLAY_NAMES),
                "total_tasks": 18,
                "tasks_with_retries": 1,
                "total_retry_attempts": 3,
                "retry_success_rate": 0.0,
                "most_retried_task": {"name": ft["task"], "host": None, "retry_count": 3},
                "analysis_timestamp": (finished + timedelta(seconds=91)).isoformat(),
            },
            "plays": [
                {
                    "play_name": play,
                    "play_uuid": f"syn-{job_id}-{n:012d}",
                    "start_time": started.isoformat(),
                    "duration_seconds": spec["duration"] / len(PLAY_NAMES),
                    "task_count": 3,
                    "host_count": 1,
                    "retry_count": 3 if play == ft["play"] else 0,
                    "failed_task_count": 1 if play == ft["play"] else 0,
                    "changed_task_count": 2,
                }
                for n, play in enumerate(PLAY_NAMES)
            ],
            "task_executions": [
                {
                    "task_name": ft["task"],
                    "host_name": None,
                    "play_name": ft["play"],
                    "retry_count": 3,
                    "total_attempts": 4,
                    "final_outcome": "runner_on_failed",
                    "duration_seconds": spec["duration"] / 4,
                    "error_patterns": [ft["msg"][:60]],
                    "first_attempt": (started + timedelta(seconds=300)).isoformat(),
                    "final_completion": finished.isoformat(),
                }
            ],
            "error_patterns": [{"pattern": ft["msg"].split(":", 1)[0], "frequency": 1}],
        },
        "extraction_metadata": {
            "scratch_directory": f"/var/spool/aap2-etl/synthetic/scratch/extract/job_{job_id}",
            "found_files": ["metadata.json", "events.jsonl", "retries.json"],
            "missing_files": [],
            "cluster_id": spec["cluster"],
            "extraction_mode": "all",
        },
    }
    return document


def write_logs(logs_dir: Path, run_date: datetime) -> None:
    logs_dir.mkdir(parents=True, exist_ok=True)
    for index, spec in enumerate(JOBS):
        document = build_document(spec, index, run_date)
        path = logs_dir / f"job_{spec['job_id']}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            json.dump(document, stream, ensure_ascii=False)
            stream.write("\n")
        print(f"wrote {path} ({path.stat().st_size} bytes)")


def main() -> int:
    if os.environ.get("RCA_CI_SEED_CONFIRM") != "1":
        print(
            "Refusing to seed: this script DROPs {SOURCE_DB_TABLE} and "
            "{SOURCE_DB_RESULT_TABLE}, and its settings may be discovered from "
            ".env / .claude settings pointing at a real database.\n"
            "Set RCA_CI_SEED_CONFIRM=1 to confirm the target is disposable.",
            file=sys.stderr,
        )
        return 1

    env = _settings()
    database = _database_settings(env)
    source_table = env.get("SOURCE_DB_TABLE") or "aap2_events"
    results_table = env.get("SOURCE_DB_RESULT_TABLE") or "aap2_job_results"
    logs_dir = Path(env.get("JOB_LOGS_DIR") or DEFAULT_LOGS_DIR)
    run_date = datetime.now(timezone.utc)

    write_logs(logs_dir, run_date)

    schema_sql = f"""
    DROP TABLE IF EXISTS {results_table};
    DROP TABLE IF EXISTS {source_table};

    CREATE TABLE {source_table} (
        job_id BIGINT PRIMARY KEY,
        job_name TEXT,
        error_message TEXT,
        job_started TIMESTAMPTZ,
        job_finished TIMESTAMPTZ,
        ai_processed BOOLEAN DEFAULT FALSE,
        cluster_name TEXT,
        aap2_job_results_fk_id INTEGER
    );

    CREATE TABLE {results_table} (
        id SERIAL PRIMARY KEY,
        batch_id TEXT NOT NULL,
        job_id BIGINT NOT NULL,
        status TEXT,
        root_cause_category TEXT,
        root_cause JSONB,
        recommendation JSONB,
        related_result_id INTEGER REFERENCES {results_table}(id),
        related_result_confidence TEXT
            CHECK (related_result_confidence IN ('high', 'medium', 'low')),
        confidence TEXT,
        catalog_item TEXT,
        job_duration_seconds INTEGER,
        ticket_link TEXT,
        ticket_resolve_datetime_gmt TIMESTAMPTZ,
        CONSTRAINT results_batch_job_unique UNIQUE (batch_id, job_id)
    );

    CREATE INDEX {results_table}_related_result_id_idx
        ON {results_table}(related_result_id);
    """

    conn = psycopg2.connect(**database)
    try:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
            for index, job in enumerate(JOBS):
                started, finished = _job_window(job, index, run_date)
                cur.execute(
                    f"""
                    INSERT INTO {source_table}
                        (job_id, job_name, error_message, job_started, job_finished,
                         ai_processed, cluster_name)
                    VALUES (%s, %s, %s, %s, %s, FALSE, %s)
                    """,
                    (
                        job["job_id"],
                        job["job_name"],
                        job["failed_task"]["msg"],
                        started,
                        finished,
                        job["cluster"],
                    ),
                )
        conn.commit()
    finally:
        conn.close()

    print(f"Seeded {len(JOBS)} synthetic job event(s) into {database['dbname']}.{source_table}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
