"""Regression tests for schema guidance and failed SDK results."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import claude_agent_sdk
import pytest

from rca.analysis.pipeline import AnalysisArtifacts
from rca.batch import orchestrator
from rca.config import Config
from rca.models import AnalysisSummary


def _config(tmp_path: Path) -> Config:
    return Config.from_env(
        environment={
            "RCA_STATE_DIR": str(tmp_path),
            "JUMPBOX_URI": "agent@jumpbox.example.test",
            "SSH_JUMPBOX_ALIAS": "ci-jumpbox",
        },
        env_file=tmp_path / "missing.env",
    )


def _artifacts(tmp_path: Path, job_id: str = "101") -> AnalysisArtifacts:
    analysis_dir = tmp_path / ".analysis" / job_id
    analysis_dir.mkdir(parents=True, exist_ok=True)
    return AnalysisArtifacts(job_id, analysis_dir, {}, {}, {}, {})


def _summary(job_id: str = "101") -> dict:
    return {
        "job_id": job_id,
        "root_cause": {
            "summary": "The worker could not be reached.",
            "category": "network",
            "confidence": "high",
        },
        "correlation": {
            "method": "guid",
            "identifiers": {"guid": "guid-1", "namespace": None},
        },
        "evidence": [
            {
                "source": "aap_job",
                "timestamp": "2026-01-01T00:00:00Z",
                "message": "Connection timed out.",
            }
        ],
        "recommendations": [{"priority": "high", "action": "Restore connectivity."}],
    }


def _save(artifacts: AnalysisArtifacts, summary: dict) -> Path:
    path = artifacts.analysis_dir / "step5_analysis_summary.json"
    path.write_text(json.dumps(summary), encoding="utf-8")
    return path


def _terminal_message(*, is_error: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        is_error=is_error,
        total_cost_usd=0.125,
        usage={"input_tokens": 12, "output_tokens": 4},
        model_usage={"claude-test": {"inputTokens": 12, "outputTokens": 4}},
        result="structured output failed" if is_error else "summary written",
        structured_output=None,
        errors=["structured output failed"] if is_error else [],
        session_id="job-session",
    )


def test_synthesis_prompt_includes_the_model_generated_schema(tmp_path: Path) -> None:
    prompt = orchestrator._synthesis_prompt("101", _artifacts(tmp_path))
    schema_text = prompt.split("```json\n", 1)[1].split("```", 1)[0]

    assert json.loads(schema_text) == AnalysisSummary.model_json_schema()
    assert 'Include the exact job ID `"101"`' in prompt


@pytest.mark.parametrize("file_state", ["valid", "invalid", "missing", "stale", "wrong_job"])
@pytest.mark.parametrize("error_site", ["stream", "cleanup"])
def test_terminal_sdk_error_cannot_recover_from_a_model_written_summary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    file_state: str,
    error_site: str,
) -> None:
    config = _config(tmp_path)
    artifacts = _artifacts(tmp_path)
    if file_state == "stale":
        _save(artifacts, _summary())
    message = _terminal_message()
    clients = []

    class FakeSDK:
        ResultError = claude_agent_sdk.ResultError
        ClaudeAgentOptions = SimpleNamespace

        class ClaudeSDKClient:
            def __init__(self, *, options):
                self.options = options
                self.queried = False
                self.exited = False
                clients.append(self)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                self.exited = True
                if error_site == "cleanup":
                    raise claude_agent_sdk.ResultError(
                        "Claude Code returned an error result", exit_code=1
                    )

            async def query(self, prompt):
                self.queried = True

            async def receive_response(self):
                assert self.queried
                if file_state == "valid":
                    _save(artifacts, _summary())
                elif file_state == "invalid":
                    _save(artifacts, {"job_id": "101"})
                elif file_state == "wrong_job":
                    _save(artifacts, _summary("202"))
                yield message
                if error_site == "stream":
                    raise claude_agent_sdk.ResultError(
                        "Claude Code returned an error result", exit_code=1
                    )

    monkeypatch.setattr(orchestrator, "_load_agent_sdk", FakeSDK)
    monkeypatch.setattr(orchestrator, "run_analysis", Mock(return_value=artifacts))
    upload = Mock(return_value=True)
    monkeypatch.setattr(orchestrator, "upload_to_jumpbox", upload)

    execution = asyncio.run(
        orchestrator._analyze_jobs([101], config, object(), cwd=tmp_path)
    )[0]

    assert execution.cost_usd == message.total_cost_usd
    assert execution.usage == message.usage
    assert execution.model_usage == message.model_usage
    assert len(clients) == 1
    assert clients[0].queried
    assert clients[0].exited
    assert clients[0].options.output_format["schema"]["properties"]["job_id"]["enum"] == ["101"]
    assert "StructuredOutput" in clients[0].options.allowed_tools
    assert execution.status == "failed"
    assert "structured output failed" in execution.error
    upload.assert_not_called()
    assert not (artifacts.analysis_dir / "step5_analysis_summary.json").exists()


@pytest.mark.parametrize(
    "failure_mode", ["no_terminal", "success_terminal", "transport_error"]
)
@pytest.mark.parametrize("error_site", ["stream", "cleanup"])
def test_sdk_queries_propagate_unrelated_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure_mode: str,
    error_site: str,
) -> None:
    expected_error = (
        claude_agent_sdk.ProcessError("transport failed", exit_code=2)
        if failure_mode == "transport_error"
        else claude_agent_sdk.ResultError("terminal error", exit_code=1)
    )

    class FakeSDK:
        ResultError = claude_agent_sdk.ResultError
        ClaudeAgentOptions = SimpleNamespace

        class ClaudeSDKClient:
            def __init__(self, *, options):
                self.options = options

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                if error_site == "cleanup":
                    raise expected_error

            async def query(self, prompt):
                pass

            async def receive_response(self):
                if failure_mode != "no_terminal":
                    yield _terminal_message(is_error=failure_mode != "success_terminal")
                if error_site == "stream":
                    raise expected_error

    monkeypatch.setattr(orchestrator, "_load_agent_sdk", FakeSDK)
    with pytest.raises(type(expected_error)) as raised:
        asyncio.run(
            orchestrator._run_sdk_query(
                "test",
                _config(tmp_path),
                cwd=tmp_path,
                semaphore=asyncio.Semaphore(1),
            )
        )
    assert raised.value is expected_error
