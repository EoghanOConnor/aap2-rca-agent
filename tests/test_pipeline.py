"""Tests for the deterministic analysis pipeline entry conditions."""

import socket
import urllib.error

import pytest

from rca.analysis.github_fetcher import GitHubAuthenticationError
from rca.analysis.pipeline import AnalysisPipelineError, run_analysis
from rca.config import Config


def test_fetch_with_job_log_requires_job_id(tmp_path) -> None:
    job_log = tmp_path / "job.json"
    job_log.write_text("{}", encoding="utf-8")
    config = Config.from_env(environment={"RCA_STATE_DIR": str(tmp_path)})

    with pytest.raises(AnalysisPipelineError, match="--fetch requires --job-id"):
        run_analysis(config=config, job_log=job_log, fetch=True)


def test_github_authentication_failure_skips_step_four(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_log = tmp_path / "job.json"
    job_log.write_text("{}", encoding="utf-8")
    config = Config.from_env(
        environment={
            "RCA_STATE_DIR": str(tmp_path),
            "GITHUB_TOKEN": "invalid-token",
        }
    )

    class FailingAnalyzer:
        def __init__(self, *_args, **_kwargs):
            pass

        def run(self):
            raise GitHubAuthenticationError("GitHub API authentication failed")

    monkeypatch.setattr("rca.analysis.pipeline.GitHubAnalyzer", FailingAnalyzer)

    artifacts = run_analysis(config=config, job_log=job_log)

    assert artifacts.github_fetch_history["skipped"] is True
    assert "authentication failed" in artifacts.github_fetch_history["reason"]


def test_splunk_unavailable_skips_step_two(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_log = tmp_path / "job.json"
    job_log.write_text("{}", encoding="utf-8")
    config = Config.from_env(
        environment={
            "RCA_STATE_DIR": str(tmp_path),
            "SPLUNK_HOST": "https://splunk.invalid",
            "SPLUNK_USERNAME": "user",
            "SPLUNK_PASSWORD": "password",
        }
    )

    def unavailable(*_args, **_kwargs):
        raise urllib.error.URLError(
            socket.gaierror(-2, "Name or service not known")
        )

    monkeypatch.setattr("rca.analysis.pipeline.fetch_correlated_logs", unavailable)

    artifacts = run_analysis(config=config, job_log=job_log)

    assert artifacts.splunk_logs["skipped"] is True
    assert "Name or service not known" in artifacts.splunk_logs["reason"]


UUID = "123e4567-e89b-12d3-a456-426614174000"


def test_cost_estimate_runs_as_step_4c(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    job_log = tmp_path / "job.json"
    job_log.write_text(
        '{"metadata": {"job_metadata": {"job_id": "1", "job_name": "RHPDS x-' + UUID + '-provision"}}}',
        encoding="utf-8",
    )
    config = Config.from_env(
        environment={"RCA_STATE_DIR": str(tmp_path), "REPORTING_MCP_TOKEN": "t"}
    )
    seen = {}

    def fake_estimate(_mcp, uuid, include_baseline=False):
        seen["uuid"] = uuid
        return {"estimate": {"cost_mid": 1.0}}

    monkeypatch.setattr("rca.analysis.pipeline.cost_estimate.connect", lambda _env: object())
    monkeypatch.setattr("rca.analysis.pipeline.cost_estimate.estimate", fake_estimate)

    artifacts = run_analysis(config=config, job_log=job_log, job_id="1")

    assert seen["uuid"] == UUID
    assert artifacts.cost_estimate == {"estimate": {"cost_mid": 1.0}}
    assert (artifacts.analysis_dir / "step4c_cost_estimate.json").is_file()


def test_cost_estimate_skipped_without_token(tmp_path) -> None:
    job_log = tmp_path / "job.json"
    job_log.write_text(
        '{"metadata": {"job_metadata": {"job_id": "1", "job_name": "x-' + UUID + '"}}}',
        encoding="utf-8",
    )
    config = Config.from_env(environment={"RCA_STATE_DIR": str(tmp_path)})

    artifacts = run_analysis(config=config, job_log=job_log, job_id="1")

    assert artifacts.cost_estimate["skipped"] is True
    assert "REPORTING_MCP_TOKEN" in artifacts.cost_estimate["reason"]
