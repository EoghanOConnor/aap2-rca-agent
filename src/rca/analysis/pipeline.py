"""Callable deterministic job-analysis pipeline (steps 1–4)."""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rca.config import Config

from .bastion_resolver import (
    prepare_bastion_for_fetch,
    resolve_bastion_for_job,
    resolve_remote_log_dir,
)
from .correlator import build_correlation_timeline, fetch_correlated_logs
from .github_fetcher import GitHubAnalyzer, GitHubAuthenticationError, GitHubClient
from .job_parser import parse_job_log
from .log_fetcher import fetch_job_log

logger = logging.getLogger(__name__)


class AnalysisPipelineError(RuntimeError):
    """A deterministic analysis step could not complete."""


@dataclass(frozen=True)
class AnalysisArtifacts:
    job_id: str
    analysis_dir: Path
    job_context: dict[str, Any]
    splunk_logs: dict[str, Any]
    correlation: dict[str, Any]
    github_fetch_history: dict[str, Any]


def get_step_name(step: int) -> str:
    return {
        1: "job_context",
        2: "splunk_logs",
        3: "correlation",
        4: "github_fetch_history",
        5: "analysis_summary",
    }.get(step, f"step{step}")


def save_step(analysis_dir: Path, step: int, data: dict[str, Any]) -> Path:
    """Write one analysis artifact as JSON."""
    analysis_dir.mkdir(parents=True, exist_ok=True)
    output_path = analysis_dir / f"step{step}_{get_step_name(step)}.json"
    with output_path.open("w", encoding="utf-8") as stream:
        json.dump(data, stream, indent=2, ensure_ascii=False, default=str)
    return output_path


def load_step(analysis_dir: Path, step: int) -> dict[str, Any] | None:
    path = analysis_dir / f"step{step}_{get_step_name(step)}.json"
    if not path.is_file():
        return None
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def _resolve_job_log(
    *,
    config: Config,
    job_id: str | None,
    job_log: str | Path | None,
    fetch: bool,
    db_pool: Any | None = None,
) -> tuple[Path, str | None]:
    if fetch and not job_id:
        raise AnalysisPipelineError("--fetch requires --job-id (it has no effect with --job-log)")

    if job_log is not None:
        path = Path(job_log).expanduser()
        if not path.is_file():
            raise AnalysisPipelineError(f"Job log file not found: {path}")
        return path, job_id

    if not job_id:
        raise AnalysisPipelineError("Either --job-log or --job-id is required")

    path = config.find_job_log(job_id)
    if path:
        return path, job_id
    if not fetch:
        if config.job_logs_dir:
            raise AnalysisPipelineError(
                f"No log file found for job {job_id} in {config.job_logs_dir}; use --fetch to retrieve it"
            )
        raise AnalysisPipelineError("JOB_LOGS_DIR is not configured; use --job-log or set JOB_LOGS_DIR")
    if not config.job_logs_dir:
        raise AnalysisPipelineError("--fetch requires JOB_LOGS_DIR to be configured")

    logger.info("[Fetch] Resolving bastion and retrieving log for job_id=%s", job_id)
    try:
        if db_pool is None:
            target = resolve_bastion_for_job(config, job_id)
        else:
            target = resolve_bastion_for_job(config, job_id, db_pool=db_pool)
        prepare_bastion_for_fetch(config, target)
        remote_dir = resolve_remote_log_dir(target, config)
        fetched_files = fetch_job_log(job_id, config.job_logs_dir, target.remote_host, remote_dir)
    except (
        FileNotFoundError,
        OSError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        ValueError,
    ) as exc:
        raise AnalysisPipelineError(f"Failed to fetch job log: {exc}") from exc

    path = config.find_job_log(job_id)
    if path is None:
        raise AnalysisPipelineError(
            f"Log fetch returned {len(fetched_files)} file(s), but no job_{job_id} log exists in "
            f"{config.job_logs_dir}"
        )
    return path, job_id


def load_job_context(
    *,
    config: Config,
    job_id: str | None = None,
    job_log: str | Path | None = None,
    fetch: bool = False,
    db_pool: Any | None = None,
) -> tuple[Path, str | None, dict[str, Any]]:
    """Resolve a job log and parse its step-one context."""

    path, requested_job_id = _resolve_job_log(
        config=config,
        job_id=job_id,
        job_log=job_log,
        fetch=fetch,
        db_pool=db_pool,
    )
    try:
        context = parse_job_log(path)
    except Exception as exc:
        raise AnalysisPipelineError(f"Could not parse job log {path}: {exc}") from exc
    return path, requested_job_id, context


def run_analysis(
    *,
    config: Config,
    job_id: str | None = None,
    job_log: str | Path | None = None,
    fetch: bool = False,
    db_pool: Any | None = None,
) -> AnalysisArtifacts:
    """Run steps 1–4 and return their artifacts for the caller."""
    job_log_path, requested_job_id, job_context = load_job_context(
        config=config,
        job_id=job_id,
        job_log=job_log,
        fetch=fetch,
        db_pool=db_pool,
    )

    parsed_job_id = str(requested_job_id or job_context.get("job_id") or "unknown")
    analysis_dir = config.analysis_dir / parsed_job_id
    save_step(analysis_dir, 1, job_context)

    splunk_errors = config.validate_splunk()
    if splunk_errors:
        splunk_logs: dict[str, Any] = {
            "job_id": parsed_job_id,
            "ocp_logs": [],
            "error_logs": [],
            "pods_found": [],
            "skipped": True,
            "reason": "; ".join(splunk_errors),
        }
    else:
        try:
            splunk_logs = fetch_correlated_logs(config, job_context)
        except Exception as exc:
            logger.warning("[Step 2] Splunk query failed for job_id=%s: %s", parsed_job_id, exc)
            splunk_logs = {
                "job_id": parsed_job_id,
                "ocp_logs": [],
                "error_logs": [],
                "pods_found": [],
                "skipped": True,
                "reason": f"Splunk is unavailable: {exc}",
                "errors": [str(exc)],
            }
    save_step(analysis_dir, 2, splunk_logs)

    correlation = build_correlation_timeline(job_context, splunk_logs)
    save_step(analysis_dir, 3, correlation)

    if not config.github_token or config.github_token == "your-github-token":
        github_fetch_history: dict[str, Any] = {
            "job_id": parsed_job_id,
            "skipped": True,
            "reason": "GITHUB_TOKEN is not configured",
            "github_fetches": [],
            "fetched_configs": {},
        }
    else:
        try:
            github_fetch_history = GitHubAnalyzer(
                parsed_job_id,
                analysis_dir,
                GitHubClient(config.github_token),
            ).run()
        except GitHubAuthenticationError as exc:
            logger.warning("[Step 4] GitHub authentication failed for job_id=%s: %s", parsed_job_id, exc)
            github_fetch_history = {
                "job_id": parsed_job_id,
                "skipped": True,
                "reason": f"GitHub API authentication failed: {exc}",
                "github_fetches": [],
                "fetched_configs": {},
            }
        except Exception as exc:
            raise AnalysisPipelineError(f"Error fetching GitHub files: {exc}") from exc
    save_step(analysis_dir, 4, github_fetch_history)

    return AnalysisArtifacts(
        job_id=parsed_job_id,
        analysis_dir=analysis_dir,
        job_context=job_context,
        splunk_logs=splunk_logs,
        correlation=correlation,
        github_fetch_history=github_fetch_history,
    )
