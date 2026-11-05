#!/usr/bin/env python3
"""Manual root-cause analysis and debugging commands."""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

from rca.analysis import cost_estimate
from rca.analysis.jumpbox_io import upload_to_jumpbox
from rca.analysis.pipeline import AnalysisPipelineError, get_step_name, run_analysis
from rca.analysis.setup import find_repo_root, print_checks, run_checks
from rca.analysis.splunk_client import SplunkClient
from rca.config import Config

logger = logging.getLogger("rca.analysis")


def _project_root() -> Path:
    """Resolve the active checkout for project-scoped setup and MLflow hooks."""
    cwd = Path.cwd()
    return find_repo_root(cwd) or find_repo_root(Path(__file__).resolve().parent) or cwd


def _write_json(data: Any, output: str | None = None) -> None:
    rendered = json.dumps(data, indent=2, ensure_ascii=False, default=str)
    if output:
        Path(output).write_text(rendered + "\n", encoding="utf-8")
        print(f"Output written to: {output}")
    else:
        print(rendered)


def cmd_analyze(args: argparse.Namespace, config: Config) -> int:
    try:
        artifacts = run_analysis(
            config=config,
            job_id=args.job_id,
            job_log=args.job_log,
            fetch=args.fetch,
        )
    except AnalysisPipelineError as exc:
        logger.error("[ERROR] %s", exc)
        return 1

    job = artifacts.job_context
    correlation = artifacts.correlation.get("correlation", {})
    print("=" * 60)
    print("Analysis Complete")
    print("=" * 60)
    print(f"Job ID: {artifacts.job_id}")
    print(f"Status: {job.get('status', '')}")
    print(f"GUID: {job.get('guid', '')}")
    print(f"Namespace: {job.get('namespace', '')}")
    print(f"Correlation confidence: {correlation.get('confidence', 'none')}")
    print(f"Analysis directory: {artifacts.analysis_dir}")
    for step in range(1, 5):
        print(f"  - step{step}_{get_step_name(step)}.json")
    return 0


def cmd_cost(args: argparse.Namespace, config: Config) -> int:
    analysis_dir = config.analysis_dir / args.job_id
    uuid = args.provision_uuid
    if not uuid:
        context_path = analysis_dir / "step1_job_context.json"
        if not context_path.is_file():
            logger.error("No step1 for job %s; run analyze first or pass --provision-uuid", args.job_id)
            return 1
        job_name = json.loads(context_path.read_text(encoding="utf-8")).get("job_name", "")
        uuid = cost_estimate.provision_uuid_from_job_name(job_name)
        if not uuid:
            logger.error("No provision UUID in job name %r; pass --provision-uuid", job_name)
            return 1
    try:
        result = cost_estimate.estimate(
            cost_estimate.connect(config.environment), uuid, include_baseline=args.baseline
        )
    except cost_estimate.CostEstimateError as exc:
        logger.error("[ERROR] %s", exc)
        return 1
    except Exception as exc:  # network/MCP failure: report, never guess a number
        logger.error("[ERROR] cost estimate failed: %s: %s", type(exc).__name__, exc)
        return 1
    analysis_dir.mkdir(parents=True, exist_ok=True)
    out = analysis_dir / "step4c_cost_estimate.json"
    out.write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    print(cost_estimate.render(result))
    print(f"\nSaved: {out}")
    return 0


def cmd_parse(args: argparse.Namespace) -> int:
    from rca.analysis.job_parser import parse_job_log

    path = Path(args.job_log).expanduser()
    if not path.is_file():
        logger.error("Job log file not found: %s", path)
        return 1
    try:
        _write_json(parse_job_log(path), args.output)
    except Exception as exc:
        logger.error("Could not parse %s: %s", path, exc)
        return 1
    return 0


def cmd_query(args: argparse.Namespace, config: Config) -> int:
    errors = config.validate_splunk()
    if errors:
        logger.error("%s", "; ".join(errors))
        return 1
    try:
        results = SplunkClient(config).query(
            args.query,
            earliest=args.earliest,
            latest=args.latest,
            max_results=args.max_results,
        )
    except Exception as exc:
        logger.error("Splunk query failed: %s", exc)
        return 1
    if args.output:
        _write_json(results, args.output)
    else:
        print(f"Found {len(results)} results")
        for row in results[:10]:
            print(f"{row.get('_time', '')}: {str(row.get('_raw', ''))[:200]}")
    return 0


def cmd_setup(config: Config, as_json: bool) -> int:
    base_dir = Path(__file__).resolve().parent
    with patch.dict(os.environ, config.environment):
        results = run_checks(base_dir, _project_root())
    if as_json:
        print(json.dumps(results, indent=2))
    else:
        issues = print_checks(results)
        return 0 if issues == 0 else 1
    return 0 if all(item["status"] == "ok" for item in results) else 1


def _run_mlflow_autolog(project_root: Path, environment: dict[str, str]) -> None:
    """Register MLflow's Claude Code hook for the active project."""
    try:
        tracking_uri = environment.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
        experiment_name = environment.get("MLFLOW_EXPERIMENT_NAME", "Default")
        result = subprocess.run(
            ["mlflow", "autolog", "claude", "-u", tracking_uri, "-n", experiment_name],
            cwd=str(project_root),
            env=environment,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0 and result.stdout:
            print(f"\n{result.stdout.strip()}")
    except Exception:
        # Autolog setup is best-effort; analysis commands must remain usable
        # when MLflow or the Claude Code CLI is not installed.
        pass


def cmd_status(args: argparse.Namespace, config: Config) -> int:
    analysis_dir = config.analysis_dir / args.job_id
    if not analysis_dir.is_dir():
        logger.error("No analysis found for job %s", args.job_id)
        return 1
    print(f"Analysis directory: {analysis_dir}")
    for step in range(1, 6):
        path = analysis_dir / f"step{step}_{get_step_name(step)}.json"
        marker = "x" if path.is_file() else " "
        suffix = f" ({path.stat().st_size} bytes)" if path.is_file() else ""
        print(f"  [{marker}] Step {step}: {path.name}{suffix}")
    return 0


def cmd_upload(args: argparse.Namespace, config: Config) -> int:
    success = upload_to_jumpbox(
        args.job_id,
        config.analysis_dir / args.job_id,
        config.jumpbox_uri,
        config.environment.get("CLAUDE_SESSION_ID", "unknown"),
        ssh_jumpbox_alias=config.environment.get("SSH_JUMPBOX_ALIAS"),
    )
    return 0 if success else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rca-analyze",
        description="Run the deterministic RCA preparation steps or a debugging command",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    analyze = subparsers.add_parser("analyze", help="Run deterministic analysis steps 1–4")
    analyze.add_argument("--job-log", help="Path to a job log file (.json or .json.gz)")
    analyze.add_argument("--job-id", help="Job ID (searches JOB_LOGS_DIR)")
    analyze.add_argument("--fetch", action="store_true", help="Fetch a missing job log over SSH")

    parse = subparsers.add_parser("parse", help="Parse a job log only")
    parse.add_argument("--job-log", required=True, help="Path to a job log file")
    parse.add_argument("--output", "-o", help="Write JSON to this file instead of stdout")

    query = subparsers.add_parser("query", help="Run an ad-hoc Splunk query")
    query.add_argument("query", help="Splunk search query")
    query.add_argument("--earliest", default="-24h")
    query.add_argument("--latest", default="now")
    query.add_argument("--max-results", type=int, default=100)
    query.add_argument("--output", "-o")

    setup = subparsers.add_parser("setup", help="Check prerequisites")
    setup.add_argument("--json", action="store_true", help="Output checks as JSON")

    cost = subparsers.add_parser("cost", help="Estimate the cost of the failing provision")
    cost.add_argument("--job-id", required=True)
    cost.add_argument("--provision-uuid", help="Override the UUID taken from the job name")
    cost.add_argument("--baseline", action="store_true", help="Also show the catalog baseline")

    status = subparsers.add_parser("status", help="Show saved analysis artifacts")
    status.add_argument("job_id")

    upload = subparsers.add_parser("upload", help="Upload analysis files to the jumpbox")
    upload.add_argument("--job-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = build_parser().parse_args(argv)
    try:
        config = Config.from_env()
    except (OSError, ValueError, SystemExit) as exc:
        logger.error("Could not load RCA configuration: %s", exc)
        return 1

    if args.command == "analyze":
        exit_code = cmd_analyze(args, config)
    elif args.command == "parse":
        exit_code = cmd_parse(args)
    elif args.command == "query":
        exit_code = cmd_query(args, config)
    elif args.command == "setup":
        exit_code = cmd_setup(config, args.json)
    elif args.command == "cost":
        exit_code = cmd_cost(args, config)
    elif args.command == "status":
        exit_code = cmd_status(args, config)
    elif args.command == "upload":
        exit_code = cmd_upload(args, config)
    else:
        return 2

    _run_mlflow_autolog(_project_root(), config.environment)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
