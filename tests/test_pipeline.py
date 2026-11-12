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
