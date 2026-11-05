"""Unit tests for the Python Agent SDK batch coordinator."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import datetime, timezone
from importlib.resources import files
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import jsonschema

from rca.analysis.pipeline import AnalysisArtifacts
from rca.analysis import tracing
from rca.batch import orchestrator
from rca.config import Config
from rca.models import AnalysisSummary, BatchSemanticOutput


def _config(
    tmp_path: Path,
    *,
    max_parallel_jobs: int = 2,
    job_logs_dir: Path | None = None,
) -> Config:
    environment = {
        "RCA_STATE_DIR": str(tmp_path),
        "RCA_MAX_PARALLEL_JOBS": str(max_parallel_jobs),
        "SOURCE_DB_NAME": "rca",
        "SOURCE_DB_USER": "agent",
        "SOURCE_DB_PASSWORD": "not-a-real-secret",
        "SOURCE_DB_TABLE": "aap2_events",
        "SOURCE_DB_RESULT_TABLE": "aap2_job_results",
        "JUMPBOX_URI": "agent@jumpbox.example.test",
        "SSH_JUMPBOX_ALIAS": "ci-jumpbox",
    }
    if job_logs_dir is not None:
        environment["JOB_LOGS_DIR"] = str(job_logs_dir)
    return Config.from_env(
        environment=environment,
        env_file=tmp_path / "missing.env",
    )


def _summary(job_id: str) -> dict[str, object]:
    return {
        "job_id": job_id,
        "analyzed_at": "2026-01-01T00:00:00Z",
        "root_cause": {
            "summary": "A test failure",
            "category": "infrastructure",
            "confidence": "high",
        },
        "correlation": {"method": "test", "identifiers": {}},
        "evidence": [
            {
                "source": "aap_job",
                "timestamp": "2026-01-01T00:00:00Z",
                "message": "A test failure occurred",
            }
        ],
        "causal_chain": [],
        "misidentifications": [],
        "recommendations": [],
    }


def _write_summary(analysis_dir: Path) -> None:
    (analysis_dir / "step5_analysis_summary.json").write_text(
        json.dumps(_summary(analysis_dir.name)), encoding="utf-8"
    )


def test_main_parses_batch_options_and_calls_python_orchestrator(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    run_batch = Mock(return_value=0)
    monkeypatch.setattr(orchestrator.Config, "from_env", classmethod(lambda cls: config))
    monkeypatch.setattr(orchestrator, "run_batch", run_batch)

    assert (
        orchestrator.main(
            ["--since", "2026-10-01 10:00:00", "--limit", "7", "--no-pre-filter"]
        )
        == 0
    )

    run_batch.assert_called_once_with(
        config,
        since="2026-10-01 10:00:00",
        limit=7,
        no_pre_filter=True,
        dry_run=False,
    )


def test_pre_analysis_context_loader_fetches_missing_logs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    job_logs_dir = tmp_path / "logs"
    job_logs_dir.mkdir()
    config = _config(tmp_path, job_logs_dir=job_logs_dir)
    pool = object()

    def fake_resolve_job_log(
        *,
        config: Config,
        job_id: str | None,
        job_log: str | Path | None,
        fetch: bool,
        db_pool: object,
    ) -> tuple[Path, str | None]:
        assert fetch is True
        assert job_id == "101"
        assert db_pool is pool
        log_path = job_logs_dir / "job_101.json"
        log_path.write_text(
            json.dumps(
                {
                    "events": [
                        {
                            "failed": True,
                            "event": "runner_on_failed",
                            "task": "Install dependencies",
                            "play": "Setup",
                            "role": "common",
                            "created": "2026-01-01T00:00:00Z",
                            "event_data": {
                                "res": {"msg": "Connection reset by peer"},
                                "task_path": "/roles/common/tasks/main.yml:10",
                                "task_action": "yum",
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        return log_path, job_id

    monkeypatch.setattr(
        "rca.analysis.pipeline._resolve_job_log", fake_resolve_job_log
    )

    contexts, job_log_paths = orchestrator._load_pre_analysis_contexts(
        [101], config, pool
    )

    assert contexts[101]["failed_tasks"][0]["task"] == "Install dependencies"
    assert job_log_paths == {101: job_logs_dir / "job_101.json"}


def test_historical_task_signatures_load_from_saved_analysis_artifacts(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    analysis_dir = config.analysis_dir / "77"
    analysis_dir.mkdir(parents=True)
    (analysis_dir / "step1_job_context.json").write_text(
        json.dumps(
            {
                "failed_tasks": [
                    {
                        "play": "Setup",
                        "role": "common",
                        "task": "Install dependencies",
                        "task_action": "yum",
                        "task_path": "/roles/common/tasks/main.yml",
                        "error_message": "Connection reset by peer",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    results = [{"id": 77, "job_id": 77}]

    orchestrator._attach_historical_task_signatures(results, config)

    assert results[0]["task_signature"].identity_key == (
        "setup",
        "common",
        "install dependencies",
        "yum",
        "/roles/common/tasks/main.yml",
    )


def test_historical_task_signatures_fall_back_to_local_job_logs(
    tmp_path: Path,
) -> None:
    job_logs_dir = tmp_path / "logs"
    job_logs_dir.mkdir()
    (job_logs_dir / "job_77.json").write_text(
        json.dumps(
            {
                "events": [
                    {
                        "failed": True,
                        "event": "runner_on_failed",
                        "task": "Install dependencies",
                        "play": "Setup",
                        "role": "",
                        "created": "2026-01-01T00:00:00Z",
                        "event_data": {
                            "res": {"msg": "Connection reset by peer"},
                            "task_path": "/roles/common/tasks/main.yml:10",
                            "task_action": "yum",
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    config = _config(tmp_path, job_logs_dir=job_logs_dir)
    results = [{"id": 77, "job_id": 77}]

    orchestrator._attach_historical_task_signatures(results, config)

    assert results[0]["task_signature"].identity_key == (
        "setup",
        "",
        "install dependencies",
        "yum",
        "/roles/common/tasks/main.yml",
    )


def test_main_passes_dry_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    run_batch = Mock(return_value=0)
    monkeypatch.setattr(orchestrator.Config, "from_env", classmethod(lambda cls: config))
    monkeypatch.setattr(orchestrator, "run_batch", run_batch)

    assert orchestrator.main(["--limit", "5", "--dry-run"]) == 0

    run_batch.assert_called_once_with(
        config,
        since=None,
        limit=5,
        no_pre_filter=False,
        dry_run=True,
    )


def test_cli_rejects_non_positive_limit() -> None:
    with pytest.raises(SystemExit):
        orchestrator._build_parser().parse_args(["--limit", "0"])


def test_packaged_prompts_load_without_filesystem_discovery() -> None:
    prompt_files = {
        "root_cause_analysis.md": orchestrator.RCA_INSTRUCTIONS,
        "synthesis.md": orchestrator._SYNTHESIS_PROMPT_TEMPLATE.template,
        "semantic.md": orchestrator._SEMANTIC_PROMPT_TEMPLATE.template,
    }

    for filename, prompt in prompt_files.items():
        packaged_prompt = (
            files("rca")
            .joinpath("prompts")
            .joinpath(filename)
            .read_text(encoding="utf-8")
        )
        assert packaged_prompt == prompt

    assert "root-cause analyst" in prompt_files["root_cause_analysis.md"]
    assert "step5_analysis_summary.json" in prompt_files["root_cause_analysis.md"]
    assert "Every entry requires `source`, `timestamp`, and `message`" in prompt_files[
        "root_cause_analysis.md"
    ]
    assert "use an empty string for `timestamp`" in prompt_files["root_cause_analysis.md"]
    assert "Never invent an event time" in prompt_files["root_cause_analysis.md"]
    assert "GitHub MCP verification" in prompt_files["root_cause_analysis.md"]
    assert "mcp__github__get_file_contents" in prompt_files["root_cause_analysis.md"]
    assert "mcp__github__search_code" in prompt_files["root_cause_analysis.md"]
    assert "$job_id" in prompt_files["synthesis.md"]
    assert "$analysis_dir" in prompt_files["synthesis.md"]
    assert "$job_summaries" in prompt_files["semantic.md"]
    assert "$known_issues" in prompt_files["semantic.md"]


def test_sdk_options_disable_discovery_and_use_packaged_instructions(
    tmp_path: Path,
) -> None:
    class FakeSDK:
        @staticmethod
        def ClaudeAgentOptions(**kwargs: object) -> dict[str, object]:
            return dict(kwargs)

    config = _config(tmp_path)
    options = orchestrator._make_sdk_options(
        FakeSDK(),
        config,
        cwd=tmp_path,
        system_prompt=orchestrator.RCA_INSTRUCTIONS,
    )

    assert options["setting_sources"] == []
    assert options["skills"] == []
    assert options["system_prompt"] == orchestrator.RCA_INSTRUCTIONS
    assert "Skill" not in options["allowed_tools"]
    assert options["allowed_tools"] == [
        "Bash",
        "Read",
        "Write",
        "mcp__github__search_code",
        "mcp__github__get_file_contents",
    ]


def test_semantic_sdk_options_disable_tools_and_skill_discovery(
    tmp_path: Path,
) -> None:
    class FakeSDK:
        @staticmethod
        def ClaudeAgentOptions(**kwargs: object) -> dict[str, object]:
            return dict(kwargs)

    config = _config(tmp_path)
    options = orchestrator._make_sdk_options(
        FakeSDK(),
        config,
        cwd=tmp_path,
        system_prompt=None,
    )

    assert options["setting_sources"] == []
    assert options["skills"] == []
    assert options["tools"] == []
    assert "system_prompt" not in options
    assert "allowed_tools" not in options


@pytest.mark.parametrize(
    ("schema", "system_prompt", "expected_tools"),
    [
        (AnalysisSummary.model_json_schema(), orchestrator.RCA_INSTRUCTIONS, None),
        (BatchSemanticOutput.model_json_schema(), None, []),
    ],
)
def test_model_generated_schema_reaches_sdk_options_unchanged(
    tmp_path: Path, schema: dict, system_prompt: str | None, expected_tools: list | None
) -> None:
    sdk = pytest.importorskip("claude_agent_sdk")
    original_schema = json.loads(json.dumps(schema))
    output_format = {"type": "json_schema", "schema": schema}

    options = orchestrator._make_sdk_options(
        sdk,
        _config(tmp_path),
        cwd=tmp_path,
        system_prompt=system_prompt,
        output_format=output_format,
    )

    assert options.output_format == output_format
    assert options.output_format["schema"] is schema
    assert schema == original_schema
    assert options.tools == expected_tools
    if system_prompt is None:
        assert options.allowed_tools == ["StructuredOutput"]
    else:
        assert options.allowed_tools == [
            "Bash", "Read", "Write", "mcp__github__search_code",
            "mcp__github__get_file_contents", "StructuredOutput",
        ]


def test_synthesis_prompt_does_not_invoke_a_skill(tmp_path: Path) -> None:
    analysis_dir = tmp_path / ".analysis" / "123"
    artifacts = AnalysisArtifacts("123", analysis_dir, {}, {}, {}, {})

    prompt = orchestrator._synthesis_prompt("123", artifacts)

    assert "Skill" not in prompt
    assert "Analyze AAP job 123" in prompt
    assert str(analysis_dir) in prompt
    assert "$analysis_dir" not in prompt


def test_semantic_prompt_renders_packaged_data() -> None:
    job_summaries = [{"job_id": 123, "confidence": "high"}]
    known_issues = [{"result_id": 456}]
    prompt = orchestrator._semantic_prompt(job_summaries, known_issues)

    assert "Compare these newly analyzed jobs" in prompt
    assert json.dumps(job_summaries) in prompt
    assert json.dumps(known_issues) in prompt
    assert "$job_summaries" not in prompt
    assert "$known_issues" not in prompt


def test_step5_persistence_preserves_nested_nullable_fields_and_extra_content(tmp_path: Path) -> None:
    summary = _summary("123")
    summary.update(
        analyzed_at=None,
        recommendations=[
            {
                "action": None,
                "priority": None,
                "evidence_ref": None,
                "fix": {"status": "proposed", "base_sha": "abc", "diff": None, "pr_link": None},
            },
            {"details": None, "evidence_ref": None, "fix": None},
        ],
        causal_chain=[
            {"step": 1, "relationship": "direct_cause", "statement": "Failure", "evidence_ref": None}
        ],
        misidentifications=[
            {
                "theory": "DNS",
                "why_suspected": "Connection failed",
                "why_ruled_out": "Resolution succeeded",
                "evidence_ref": None,
            }
        ],
        extra_context={"unavailable": None, "nested": [{"value": None}]},
    )
    summary["evidence"][0]["github_path"] = None
    summary["evidence"][0]["extra_details"] = {"unavailable": None}
    original = json.loads(json.dumps(summary))

    artifacts = AnalysisArtifacts("123", tmp_path, {}, {}, {}, {})
    orchestrator._write_step5_summary(artifacts, summary)

    saved = json.loads((tmp_path / "step5_analysis_summary.json").read_text())
    assert saved == original
    assert summary == original


def test_sdk_result_usage_and_semaphore_limit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    active = 0
    peak_active = 0
    option_values: list[dict[str, object]] = []

    class FakeSDK:
        @staticmethod
        def ClaudeAgentOptions(**kwargs: object) -> dict[str, object]:
            options = dict(kwargs)
            option_values.append(options)
            return options

        class ClaudeSDKClient:
            def __init__(self, options: dict[str, object]):
                self.options = options

            async def __aenter__(self):
                nonlocal active, peak_active
                active += 1
                peak_active = max(peak_active, active)
                return self

            async def __aexit__(self, *_args: object):
                nonlocal active
                active -= 1

            async def query(self, prompt: str) -> None:
                self.prompt = prompt
                await asyncio.sleep(0.01)

            async def receive_response(self):
                yield SimpleNamespace(
                    is_error=False,
                    total_cost_usd=0.125,
                    usage={"input_tokens": 12, "output_tokens": 4},
                    model_usage={
                        "claude-test-v1": {
                            "inputTokens": 12,
                            "outputTokens": 4,
                            "costUSD": 0.125,
                        }
                    },
                    result=f"completed {self.prompt}",
                    structured_output={"ok": True},
                    errors=[],
                    session_id="session-test",
                )

    monkeypatch.setattr(orchestrator, "_load_agent_sdk", lambda: FakeSDK())
    config = _config(tmp_path, max_parallel_jobs=2)
    output_format = {"type": "json_schema", "schema": orchestrator._SEMANTIC_OUTPUT_SCHEMA}

    async def run_queries():
        semaphore = asyncio.Semaphore(config.max_parallel_jobs)
        return await asyncio.gather(
            *(
                orchestrator._run_sdk_query(
                    f"job {index}",
                    config,
                    cwd=tmp_path,
                    semaphore=semaphore,
                    system_prompt=orchestrator.RCA_INSTRUCTIONS,
                    output_format=output_format,
                )
                for index in range(5)
            )
        )

    results = asyncio.run(run_queries())

    assert peak_active == 2
    assert len(results) == 5
    assert results[0].is_error is False
    assert results[0].cost_usd == 0.125
    assert results[0].usage == {"input_tokens": 12, "output_tokens": 4}
    assert results[0].model_usage == {
        "claude-test-v1": {
            "inputTokens": 12,
            "outputTokens": 4,
            "costUSD": 0.125,
        }
    }
    assert results[0].text == "completed job 0"
    assert results[0].structured_output == {"ok": True}
    assert results[0].session_id == "session-test"
    assert all(item["system_prompt"] == orchestrator.RCA_INSTRUCTIONS for item in option_values)
    assert all(item["output_format"] == output_format for item in option_values)
    assert all("StructuredOutput" in item["allowed_tools"] for item in option_values)


def test_synthesis_passes_the_model_generated_schema_to_a_single_sdk_query(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    analysis_dir = config.analysis_dir / "101"
    artifacts = AnalysisArtifacts("101", analysis_dir, {}, {}, {}, {})
    prompt = orchestrator._synthesis_prompt("101", artifacts)
    semaphore = asyncio.Semaphore(1)
    original_schema = AnalysisSummary.model_json_schema()
    sdk_result = orchestrator.SDKQueryResult(
        False,
        0.02,
        {"input_tokens": 20, "output_tokens": 5},
        "Summary returned",
        model_usage={"claude-test": {"inputTokens": 20, "costUSD": 0.02}},
        structured_output=_summary("101"),
        session_id="synthesis-session",
    )
    query = AsyncMock(return_value=sdk_result)
    monkeypatch.setattr(orchestrator, "_run_sdk_query", query)

    result = asyncio.run(
        orchestrator._run_synthesis(
            prompt,
            config,
            artifacts=artifacts,
            cwd=tmp_path,
            semaphore=semaphore,
            summary_job_id="101",
        )
    )

    expected_schema = json.loads(json.dumps(original_schema))
    expected_schema["properties"]["job_id"]["enum"] = ["101"]
    query.assert_awaited_once_with(
        prompt,
        config,
        cwd=tmp_path,
        semaphore=semaphore,
        system_prompt=orchestrator.RCA_INSTRUCTIONS,
        output_format={"type": "json_schema", "schema": expected_schema},
    )
    schema = query.await_args.kwargs["output_format"]["schema"]
    validator = jsonschema.Draft202012Validator(schema)
    validator.validate(_summary("101"))
    assert not validator.is_valid(_summary("999"))
    assert orchestrator._SUMMARY_OUTPUT_SCHEMA == original_schema
    assert result is sdk_result


@pytest.mark.parametrize("failure", ["sdk_error", "missing_output", "transport_error"])
def test_synthesis_failures_do_not_write_or_upload_summary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str
) -> None:
    config = _config(tmp_path)
    analysis_dir = config.analysis_dir / "101"
    analysis_dir.mkdir(parents=True)
    artifacts = AnalysisArtifacts("101", analysis_dir, {}, {}, {}, {})
    query = AsyncMock(
        side_effect=RuntimeError("SDK transport failed") if failure == "transport_error" else None,
        return_value=orchestrator.SDKQueryResult(
            failure == "sdk_error",
            0.02,
            {},
            "SDK request failed",
            structured_output=_summary("101") if failure == "sdk_error" else None,
            errors=("SDK request failed",) if failure == "sdk_error" else (),
        ),
    )
    uploader = Mock(return_value=True)
    monkeypatch.setattr(orchestrator, "run_analysis", Mock(return_value=artifacts))
    monkeypatch.setattr(orchestrator, "_run_sdk_query", query)
    monkeypatch.setattr(orchestrator, "upload_to_jumpbox", uploader)

    execution = asyncio.run(
        orchestrator._analyze_jobs([101], config, object(), cwd=tmp_path)
    )[0]

    assert execution.status == "failed"
    query.assert_awaited_once()
    assert not (analysis_dir / "step5_analysis_summary.json").exists()
    uploader.assert_not_called()


def test_job_failures_are_isolated_from_other_jobs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run_analysis(*, config: Config, job_id: str, fetch: bool, db_pool: object):
        assert fetch is True
        if job_id == "101":
            raise RuntimeError("missing job log")
        analysis_dir = tmp_path / ".analysis" / job_id
        analysis_dir.mkdir(parents=True)
        return AnalysisArtifacts(
            job_id=job_id,
            analysis_dir=analysis_dir,
            job_context={},
            splunk_logs={},
            correlation={},
            github_fetch_history={},
        )

    async def fake_synthesis(*args: object, **kwargs: object) -> orchestrator.SDKQueryResult:
        return orchestrator.SDKQueryResult(
            is_error=False,
            cost_usd=0.02,
            usage={"input_tokens": 20},
            model_usage={"claude-test-v1": {"inputTokens": 20, "costUSD": 0.02}},
            text="summary returned",
            structured_output=_summary("102"),
        )

    monkeypatch.setattr(orchestrator, "run_analysis", fake_run_analysis)
    monkeypatch.setattr(orchestrator, "_run_synthesis", fake_synthesis)
    monkeypatch.setattr(orchestrator, "upload_to_jumpbox", Mock(return_value=True))
    config = _config(tmp_path, max_parallel_jobs=2)

    executions = asyncio.run(
        orchestrator._analyze_jobs([101, 102], config, object(), cwd=tmp_path)
    )

    assert [execution.status for execution in executions] == ["failed", "completed"]
    assert executions[0].stage == "analysis_pipeline"
    assert "missing job log" in (executions[0].error or "")
    assert executions[1].cost_usd == 0.02
    assert executions[1].usage == {"input_tokens": 20}
    assert executions[1].model_usage == {
        "claude-test-v1": {"inputTokens": 20, "costUSD": 0.02}
    }


def test_job_concurrency_bounds_analysis_and_agent_query_as_one_unit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    active_jobs: set[str] = set()
    active_lock = threading.Lock()
    peak_active_jobs = 0

    def fake_run_analysis(*, config: Config, job_id: str, fetch: bool, db_pool: object):
        nonlocal peak_active_jobs
        assert fetch is True
        with active_lock:
            active_jobs.add(job_id)
            peak_active_jobs = max(peak_active_jobs, len(active_jobs))

        analysis_dir = tmp_path / ".analysis" / job_id
        analysis_dir.mkdir(parents=True, exist_ok=True)
        time.sleep(0.03)
        return AnalysisArtifacts(
            job_id=job_id,
            analysis_dir=analysis_dir,
            job_context={},
            splunk_logs={},
            correlation={},
            github_fetch_history={},
        )

    async def fake_synthesis(prompt: str, *args: object, **kwargs: object) -> orchestrator.SDKQueryResult:
        job_id = prompt.split("AAP job ", 1)[1].split(" ", 1)[0]
        await asyncio.sleep(0.04)
        return orchestrator.SDKQueryResult(
            is_error=False,
            cost_usd=0.01,
            usage={},
            text="summary returned",
            structured_output=_summary(job_id),
        )

    def fake_upload(job_id: str, *args: object, **kwargs: object) -> bool:
        time.sleep(0.03)
        with active_lock:
            active_jobs.remove(job_id)
        return True

    monkeypatch.setattr(orchestrator, "run_analysis", fake_run_analysis)
    monkeypatch.setattr(orchestrator, "_run_synthesis", fake_synthesis)
    monkeypatch.setattr(orchestrator, "upload_to_jumpbox", fake_upload)
    config = _config(tmp_path, max_parallel_jobs=2)

    executions = asyncio.run(
        orchestrator._analyze_jobs([101, 102, 103, 104, 105], config, object(), cwd=tmp_path)
    )

    assert all(execution.status == "completed" for execution in executions)
    assert peak_active_jobs == config.max_parallel_jobs
    assert active_jobs == set()


def test_retry_rejects_a_previous_step5_summary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    analysis_dir = config.analysis_dir / "101"
    analysis_dir.mkdir(parents=True)
    _write_summary(analysis_dir)
    artifacts = AnalysisArtifacts("101", analysis_dir, {}, {}, {}, {})
    monkeypatch.setattr(orchestrator, "run_analysis", Mock(return_value=artifacts))
    monkeypatch.setattr(
        orchestrator,
        "_run_synthesis",
        AsyncMock(return_value=orchestrator.SDKQueryResult(False, 0.02, {}, "No summary written")),
    )
    upload = Mock(return_value=True)
    monkeypatch.setattr(orchestrator, "upload_to_jumpbox", upload)

    execution = asyncio.run(
        orchestrator._analyze_jobs([101], config, object(), cwd=tmp_path)
    )[0]

    assert execution.status == "failed"
    assert execution.stage == "step5_missing"
    assert not (analysis_dir / "step5_analysis_summary.json").exists()
    upload.assert_not_called()


@pytest.mark.parametrize("upload_result", [True, False, OSError("SSH unavailable")])
def test_job_success_requires_a_verified_upload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, upload_result: bool | Exception
) -> None:
    config = _config(tmp_path)
    analysis_dir = config.analysis_dir / "101"
    analysis_dir.mkdir(parents=True)
    artifacts = AnalysisArtifacts("101", analysis_dir, {}, {}, {}, {})
    monkeypatch.setattr(orchestrator, "run_analysis", Mock(return_value=artifacts))

    async def fake_synthesis(*args: object, **kwargs: object) -> orchestrator.SDKQueryResult:
        return orchestrator.SDKQueryResult(
            False,
            0.02,
            {"input_tokens": 20},
            "summary returned",
            structured_output=_summary("101"),
            session_id="job-session",
        )

    monkeypatch.setattr(orchestrator, "_run_synthesis", fake_synthesis)
    upload = (
        Mock(side_effect=upload_result)
        if isinstance(upload_result, Exception)
        else Mock(return_value=upload_result)
    )
    monkeypatch.setattr(orchestrator, "upload_to_jumpbox", upload)

    execution = asyncio.run(
        orchestrator._analyze_jobs([101], config, object(), cwd=tmp_path)
    )[0]

    upload.assert_called_once_with(
        "101",
        analysis_dir,
        jumpbox_uri=config.jumpbox_uri,
        session_id="job-session",
        ssh_jumpbox_alias="ci-jumpbox",
    )
    assert execution.cost_usd == 0.02
    assert execution.usage == {"input_tokens": 20}
    if upload_result is True:
        assert execution.status == "completed"
    else:
        assert execution.status == "failed"
        assert execution.stage == "upload"
        assert "Analysis upload failed" in execution.error
        assert orchestrator._job_summary(execution)["status"] == "failed"


@pytest.mark.parametrize(
    ("category", "expected"),
    [("workload_bug", "application_bug"), ("credential", "secrets")],
)
def test_legacy_skill_categories_survive_report_aggregation(category: str, expected: str) -> None:
    execution = orchestrator.JobExecution(
        "101",
        "completed",
        1,
        summary={
            "root_cause": {"summary": "test", "category": category, "confidence": "high"},
            "recommendations": [{"action": "Fix the cause", "priority": "high"}],
        },
    )
    summary = orchestrator._job_summary(execution)
    assert summary["root_cause_category"] == expected
    assert orchestrator._recommendations([summary])[0]["category"] == expected


@pytest.mark.parametrize(
    ("statuses", "has_pre_match", "expected_exit"),
    [
        (["failed", "failed"], False, 1),
        (["completed", "failed"], False, 0),
        (["completed", "completed"], False, 0),
        (["failed", "failed"], True, 0),
        ([], True, 0),
    ],
)
def test_batch_exit_status_preserves_reports_and_partial_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    statuses: list[str],
    has_pre_match: bool,
    expected_exit: int,
) -> None:
    config = _config(tmp_path)
    monkeypatch.setattr(orchestrator, "ensure_jumpbox_alias", Mock())
    executions = [
        orchestrator.JobExecution(
            str(101 + index),
            status,
            1,
            summary={"root_cause": {"summary": "test"}} if status == "completed" else None,
            error="authentication failed" if status == "failed" else None,
        )
        for index, status in enumerate(statuses)
    ]
    pre_matched = [{"job_id": 103, "matched_result_id": 9}] if has_pre_match else []
    skip_targets = {103: 9} if has_pre_match else {}
    recent_results = [
        {
            "id": 9,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "root_cause_summary": "Known issue",
        }
    ]
    monkeypatch.setattr(orchestrator, "pooled_connection", lambda pool: nullcontext(Mock()))
    monkeypatch.setattr(orchestrator, "query_job_ids", Mock(return_value=[101, 102, 103]))
    monkeypatch.setattr(orchestrator, "fetch_job_metadata", Mock(return_value={}))
    monkeypatch.setattr(orchestrator, "fetch_known_issues", Mock(return_value=[{"result_id": 9}]))
    monkeypatch.setattr(orchestrator, "fetch_recent_results", Mock(return_value=recent_results))
    monkeypatch.setattr(
        orchestrator,
        "pre_analysis_gate",
        Mock(
            return_value=(
                [int(item.job_id) for item in executions],
                skip_targets,
                [],
            )
        ),
    )
    monkeypatch.setattr(orchestrator, "store_pre_matched", Mock())
    monkeypatch.setattr(orchestrator, "_analyze_jobs", AsyncMock(return_value=executions))
    monkeypatch.setattr(orchestrator, "_aggregate_semantics", AsyncMock(return_value=([], None)))
    monkeypatch.setattr(orchestrator, "_log_mlflow_usage", Mock())
    store = Mock(return_value=True)
    monkeypatch.setattr(orchestrator, "_store_batch_report", store)

    assert orchestrator.run_batch(config, pool=object(), cwd=tmp_path) == expected_exit
    if executions:
        store.assert_called_once()
        report = store.call_args.args[2]
        assert report["total_jobs_failed"] == statuses.count("failed")
        assert report["total_jobs_analyzed"] == statuses.count("completed")
        assert store.call_args.args[3].is_file()
    else:
        store.assert_not_called()


def test_failed_representative_falls_back_to_next_duplicate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    executions = [
        orchestrator.JobExecution(
            "100",
            "failed",
            1,
            error="analysis failed",
        )
    ]
    recovered = [
        orchestrator.JobExecution(
            "101",
            "completed",
            1,
            summary={"root_cause": {"summary": "recovered"}},
        )
    ]
    dupes = [
        {"job_id": 101, "representative_job_id": 100},
        {"job_id": 102, "representative_job_id": 100},
    ]
    monkeypatch.setattr(
        orchestrator,
        "_analyze_jobs",
        AsyncMock(return_value=recovered),
    )
    pool = object()

    executions, updated_dupes = orchestrator._recover_failed_representatives(
        executions,
        dupes,
        set(),
        config,
        pool,
        cwd=tmp_path,
    )

    orchestrator._analyze_jobs.assert_called_once_with(
        [101], config, pool, cwd=tmp_path, dry_run=False
    )

    assert [execution.job_id for execution in executions] == ["100", "101"]
    assert executions[-1].job_id == "101"
    assert updated_dupes == [{"job_id": 102, "representative_job_id": 101}]


def test_pre_analysis_fetches_contexts_only_for_unresolved_jobs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    pool = object()
    metadata = {
        10: {"job_name": "RHPDS platform.widget.dev-10-create"},
        11: {"job_name": "RHPDS platform.widget.dev-11-create"},
        12: {"job_name": "RHPDS platform.widget.dev-12-create"},
    }
    recent_results = [
        {
            "id": 90,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "root_cause_summary": "Known issue",
        }
    ]
    context = {"failed_tasks": [{"task": "Install dependencies"}]}
    log_path = tmp_path / "job_10.json"
    gate_task_contexts = []

    def fake_pre_analysis_gate(
        job_ids: list[int],
        job_metadata: dict[int, dict[str, object]],
        historical: list[dict[str, object]],
        *,
        use_history: bool,
        task_contexts: dict[int, dict[str, object]] | None = None,
    ) -> tuple[list[int], dict[int, int], list[dict[str, object]]]:
        gate_task_contexts.append(task_contexts)
        if task_contexts is None:
            return [10], {11: 90, 12: 90}, []
        return [], {11: 90, 12: 90}, []

    monkeypatch.setattr(orchestrator, "pooled_connection", lambda pool: nullcontext(Mock()))
    monkeypatch.setattr(orchestrator, "query_job_ids", Mock(return_value=[10, 11, 12]))
    monkeypatch.setattr(orchestrator, "fetch_job_metadata", Mock(return_value=metadata))
    monkeypatch.setattr(orchestrator, "fetch_recent_results", Mock(return_value=recent_results))
    monkeypatch.setattr(orchestrator, "fetch_known_issues", Mock(return_value=[]))
    monkeypatch.setattr(orchestrator, "_attach_historical_task_signatures", Mock())
    monkeypatch.setattr(orchestrator, "pre_analysis_gate", fake_pre_analysis_gate)
    context_loader = Mock(return_value=({10: context}, {10: log_path}))
    monkeypatch.setattr(orchestrator, "_load_pre_analysis_contexts", context_loader)
    monkeypatch.setattr(orchestrator, "store_pre_matched", Mock())
    analyze_jobs = Mock()
    monkeypatch.setattr(orchestrator, "_analyze_jobs", analyze_jobs)

    assert orchestrator.run_batch(config, pool=pool, cwd=tmp_path) == 0

    assert gate_task_contexts == [None, {10: context}]
    assert context_loader.call_args.args == ([10], config, pool)
    analyze_jobs.assert_not_called()


def test_dry_run_writes_report_without_database_or_jumpbox_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    execution = orchestrator.JobExecution(
        "101",
        "completed",
        1,
        summary={"root_cause": {"summary": "test", "category": "unknown"}},
    )
    monkeypatch.setattr(orchestrator, "pooled_connection", lambda pool: nullcontext(Mock()))
    monkeypatch.setattr(orchestrator, "query_job_ids", Mock(return_value=[101]))
    monkeypatch.setattr(orchestrator, "fetch_job_metadata", Mock(return_value={}))
    monkeypatch.setattr(orchestrator, "fetch_recent_results", Mock(return_value=[]))
    monkeypatch.setattr(orchestrator, "fetch_known_issues", Mock(return_value=[]))
    monkeypatch.setattr(
        orchestrator,
        "pre_analysis_gate",
        Mock(return_value=([101], {}, [])),
    )
    monkeypatch.setattr(orchestrator, "store_pre_matched", Mock())
    monkeypatch.setattr(
        orchestrator,
        "_analyze_jobs",
        AsyncMock(return_value=[execution]),
    )
    monkeypatch.setattr(
        orchestrator,
        "_aggregate_semantics",
        AsyncMock(return_value=([], None)),
    )
    monkeypatch.setattr(orchestrator, "_log_mlflow_usage", Mock())
    store = Mock(return_value=True)
    monkeypatch.setattr(orchestrator, "_store_batch_report", store)

    assert orchestrator.run_batch(
        config,
        pool=object(),
        cwd=tmp_path,
        dry_run=True,
    ) == 0

    orchestrator.store_pre_matched.assert_not_called()
    store.assert_not_called()
    report_files = list((tmp_path / "reports").glob("batch_*.json"))
    assert len(report_files) == 1


def test_run_batch_prepares_jumpbox_alias_before_analysis(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    execution = orchestrator.JobExecution(
        "101",
        "completed",
        1,
        summary={"root_cause": {"summary": "test", "category": "unknown"}},
    )
    order: list[str] = []
    analysis_calls: list[tuple[object, ...]] = []
    ensure_alias = Mock(side_effect=lambda _config: order.append("alias"))

    async def analyze_jobs(*args: object, **kwargs: object) -> list[orchestrator.JobExecution]:
        analysis_calls.append((*args, kwargs))
        order.append("analysis")
        return [execution]

    monkeypatch.setattr(orchestrator, "ensure_jumpbox_alias", ensure_alias)
    monkeypatch.setattr(orchestrator, "pooled_connection", lambda pool: nullcontext(Mock()))
    monkeypatch.setattr(orchestrator, "query_job_ids", Mock(return_value=[101]))
    monkeypatch.setattr(orchestrator, "fetch_job_metadata", Mock(return_value={}))
    monkeypatch.setattr(orchestrator, "fetch_recent_results", Mock(return_value=[]))
    monkeypatch.setattr(orchestrator, "fetch_known_issues", Mock(return_value=[]))
    monkeypatch.setattr(
        orchestrator,
        "pre_analysis_gate",
        Mock(return_value=([101], {}, [])),
    )
    monkeypatch.setattr(orchestrator, "_analyze_jobs", analyze_jobs)
    monkeypatch.setattr(
        orchestrator,
        "_aggregate_semantics",
        AsyncMock(return_value=([], None)),
    )
    monkeypatch.setattr(orchestrator, "_log_mlflow_usage", Mock())
    monkeypatch.setattr(
        orchestrator,
        "_store_batch_report",
        Mock(return_value=True),
    )

    assert orchestrator.run_batch(config, pool=object(), cwd=tmp_path) == 0

    ensure_alias.assert_called_once_with(config)
    assert len(analysis_calls) == 1
    assert order == ["alias", "analysis"]


def test_run_batch_dry_run_skips_jumpbox_alias_preparation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    execution = orchestrator.JobExecution(
        "101",
        "completed",
        1,
        summary={"root_cause": {"summary": "test", "category": "unknown"}},
    )
    ensure_alias = Mock()
    analyze_jobs = AsyncMock(return_value=[execution])

    monkeypatch.setattr(orchestrator, "ensure_jumpbox_alias", ensure_alias)
    monkeypatch.setattr(orchestrator, "pooled_connection", lambda pool: nullcontext(Mock()))
    monkeypatch.setattr(orchestrator, "query_job_ids", Mock(return_value=[101]))
    monkeypatch.setattr(orchestrator, "fetch_job_metadata", Mock(return_value={}))
    monkeypatch.setattr(orchestrator, "fetch_recent_results", Mock(return_value=[]))
    monkeypatch.setattr(orchestrator, "fetch_known_issues", Mock(return_value=[]))
    monkeypatch.setattr(
        orchestrator,
        "pre_analysis_gate",
        Mock(return_value=([101], {}, [])),
    )
    monkeypatch.setattr(orchestrator, "_analyze_jobs", analyze_jobs)
    monkeypatch.setattr(
        orchestrator,
        "_aggregate_semantics",
        AsyncMock(return_value=([], None)),
    )
    monkeypatch.setattr(orchestrator, "_log_mlflow_usage", Mock())
    monkeypatch.setattr(
        orchestrator,
        "_store_batch_report",
        Mock(return_value=True),
    )

    assert orchestrator.run_batch(
        config,
        pool=object(),
        cwd=tmp_path,
        dry_run=True,
    ) == 0

    ensure_alias.assert_not_called()
    analyze_jobs.assert_called_once()


def test_run_batch_stops_before_analysis_when_jumpbox_alias_preparation_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    ensure_alias = Mock(side_effect=OSError("cannot write SSH config"))
    analyze_jobs = AsyncMock()

    monkeypatch.setattr(orchestrator, "ensure_jumpbox_alias", ensure_alias)
    monkeypatch.setattr(orchestrator, "pooled_connection", lambda pool: nullcontext(Mock()))
    monkeypatch.setattr(orchestrator, "query_job_ids", Mock(return_value=[101]))
    monkeypatch.setattr(orchestrator, "fetch_job_metadata", Mock(return_value={}))
    monkeypatch.setattr(orchestrator, "fetch_recent_results", Mock(return_value=[]))
    monkeypatch.setattr(orchestrator, "fetch_known_issues", Mock(return_value=[]))
    monkeypatch.setattr(
        orchestrator,
        "pre_analysis_gate",
        Mock(return_value=([101], {}, [])),
    )
    monkeypatch.setattr(orchestrator, "_analyze_jobs", analyze_jobs)
    store = Mock(return_value=True)
    monkeypatch.setattr(orchestrator, "_store_batch_report", store)

    assert orchestrator.run_batch(config, pool=object(), cwd=tmp_path) == 1

    ensure_alias.assert_called_once_with(config)
    analyze_jobs.assert_not_called()
    store.assert_not_called()


def test_dry_run_skips_jumpbox_upload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    analysis_dir = config.analysis_dir / "101"
    analysis_dir.mkdir(parents=True)
    artifacts = AnalysisArtifacts("101", analysis_dir, {}, {}, {}, {})
    upload = Mock(return_value=True)
    monkeypatch.setattr(orchestrator, "run_analysis", Mock(return_value=artifacts))
    sdk_kwargs: dict[str, object] = {}

    async def fake_synthesis(*args: object, **kwargs: object) -> orchestrator.SDKQueryResult:
        sdk_kwargs.update(kwargs)
        return orchestrator.SDKQueryResult(
            False,
            0.02,
            {"input_tokens": 20},
            "summary returned",
            structured_output=_summary("101"),
            session_id="job-session",
        )

    monkeypatch.setattr(orchestrator, "_run_synthesis", fake_synthesis)
    monkeypatch.setattr(orchestrator, "upload_to_jumpbox", upload)

    execution = asyncio.run(
        orchestrator._execute_one_job(
            101,
            config,
            object(),
            ThreadPoolExecutor(max_workers=1),
            asyncio.Semaphore(1),
            tmp_path,
            True,
        )
    )

    upload.assert_not_called()
    assert execution.status == "completed"
    assert sdk_kwargs["artifacts"] is artifacts
    assert sdk_kwargs["summary_job_id"] == "101"
    assert execution.summary == _summary("101")
    assert json.loads((analysis_dir / "step5_analysis_summary.json").read_text()) == _summary("101")


def test_store_batch_report_links_in_one_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = Mock()
    pool = SimpleNamespace(getconn=Mock(return_value=connection), putconn=Mock())
    store = Mock(return_value=True)
    post_link = Mock()
    link_dupes = Mock()
    monkeypatch.setattr(orchestrator, "store_report", store)
    monkeypatch.setattr(orchestrator, "post_analysis_link", post_link)
    monkeypatch.setattr(orchestrator, "link_intra_batch_dupes", link_dupes)
    report = {"job_summaries": []}
    dupes = [{"job_id": 102, "representative_job_id": 101}]

    assert orchestrator._store_batch_report(pool, {}, report, Path("report.json"), dupes)

    store.assert_called_once_with(
        connection,
        {},
        report,
        filename="report.json",
        commit=False,
    )
    post_link.assert_called_once_with(connection, {}, report, commit=False)
    link_dupes.assert_called_once_with(connection, {}, dupes, commit=False)
    connection.commit.assert_called_once_with()


def test_store_batch_report_rolls_back_when_linking_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = Mock()
    connection.closed = False
    pool = SimpleNamespace(getconn=Mock(return_value=connection), putconn=Mock())
    monkeypatch.setattr(orchestrator, "store_report", Mock(return_value=True))
    monkeypatch.setattr(
        orchestrator,
        "post_analysis_link",
        Mock(side_effect=OSError("link failed")),
    )

    with pytest.raises(OSError, match="link failed"):
        orchestrator._store_batch_report(
            pool,
            {},
            {"job_summaries": []},
            Path("report.json"),
            [],
        )

    connection.commit.assert_not_called()
    connection.rollback.assert_called_once_with()


def test_normalize_semantics_validates_job_and_result_ids() -> None:
    summaries = [
        {"job_id": "job-1", "status": "analyzed", "confidence": "high"},
        {"job_id": "job-2", "status": "analyzed", "confidence": "low"},
        {"job_id": "failed", "status": "failed", "confidence": "low"},
    ]
    known_issues = [{"result_id": 9, "recurrence_count": 3}]
    output = {
        "historical_matches": [
            {
                "job_id": "job-1",
                "matches": [
                    {"matched_result_id": 9, "similarity_reasoning": "Same failure mode."}
                ],
            },
            {
                "job_id": "job-2",
                "matches": [
                    {"matched_result_id": 9, "similarity_reasoning": "Same failure mode."}
                ],
            },
            {
                "job_id": "failed",
                "matches": [
                    {"matched_result_id": 9, "similarity_reasoning": "Same failure mode."}
                ],
            },
        ],
        "cross_job_patterns": [
            {
                "pattern": "shared failure",
                "jobs": ["job-1", "job-2", "failed", "not-in-batch"],
                "description": "Both analyzed jobs show the same failure.",
            }
        ],
    }

    normalized = orchestrator._normalize_semantics(output, summaries, known_issues)

    assert summaries[0]["historical_matches"] == [
        {
            "matched_result_id": 9,
            "recurrence_count": 3,
            "similarity_reasoning": "Same failure mode.",
            "pattern_id": "9",
            "confidence": "low",
            "evidence": [],
        }
    ]
    assert summaries[1]["historical_matches"] == []
    assert summaries[2]["historical_matches"] == []
    assert normalized["cross_job_patterns"][0]["jobs"] == ["job-1", "job-2"]


def test_normalize_semantics_preserves_pattern_metadata() -> None:
    summaries = [
        {"job_id": "job-1", "status": "analyzed", "confidence": "high"},
        {"job_id": "job-2", "status": "analyzed", "confidence": "high"},
    ]
    known_issues = [
        {"result_id": 9, "recurrence_count": 2, "pattern_id": "pattern-9"}
    ]
    output = {
        "historical_matches": [
            {
                "job_id": "job-1",
                "matches": [
                    {
                        "matched_result_id": 9,
                        "similarity_reasoning": "Same failure mode.",
                        "confidence": "high",
                        "evidence": [
                            {
                                "statement_type": "observed",
                                "kind": "error_signature",
                                "description": "Both jobs report timeout from worker 7.",
                                "source": "splunk",
                            }
                        ],
                    }
                ],
            }
        ],
        "cross_job_patterns": [
            {
                "pattern": "worker timeout",
                "jobs": ["job-1", "job-2"],
                "description": "Both jobs time out in worker 7.",
                "confidence": "high",
                "evidence": [
                    {
                        "statement_type": "observed",
                        "kind": "component",
                        "description": "Worker 7 is the failing component in both jobs.",
                        "source": "splunk",
                    }
                ],
            }
        ],
    }

    normalized = orchestrator._normalize_semantics(output, summaries, known_issues)

    historical_match = summaries[0]["historical_matches"][0]
    assert historical_match["pattern_id"] == "pattern-9"
    assert historical_match["confidence"] == "high"
    assert historical_match["evidence"][0]["description"] == (
        "Both jobs report timeout from worker 7."
    )
    cross_pattern = normalized["cross_job_patterns"][0]
    assert cross_pattern["pattern_id"] == "pattern-9"
    assert cross_pattern["confidence"] == "high"
    assert cross_pattern["evidence"][0]["kind"] == "component"


def test_normalize_semantics_marks_ambiguous_pattern_low() -> None:
    summaries = [
        {"job_id": "job-1", "status": "analyzed", "confidence": "high"},
        {"job_id": "job-2", "status": "analyzed", "confidence": "high"},
    ]
    known_issues = [
        {"result_id": 7, "recurrence_count": 1, "pattern_id": "pattern-7"},
        {"result_id": 9, "recurrence_count": 1, "pattern_id": "pattern-9"},
    ]
    output = {
        "historical_matches": [
            {
                "job_id": "job-1",
                "matches": [
                    {
                        "matched_result_id": 7,
                        "similarity_reasoning": "Same failure mode.",
                        "confidence": "high",
                    }
                ],
            },
            {
                "job_id": "job-2",
                "matches": [
                    {
                        "matched_result_id": 9,
                        "similarity_reasoning": "Same failure mode.",
                        "confidence": "high",
                    }
                ],
            },
        ],
        "cross_job_patterns": [
            {
                "pattern": "ambiguous failure",
                "jobs": ["job-1", "job-2"],
                "description": "The jobs share a failure mode.",
                "confidence": "high",
            }
        ],
    }

    normalized = orchestrator._normalize_semantics(output, summaries, known_issues)

    cross_pattern = normalized["cross_job_patterns"][0]
    assert cross_pattern["pattern_id"] == "pattern-7"
    assert cross_pattern["confidence"] == "low"


def test_normalize_semantics_picks_numeric_min_pattern_id() -> None:
    # Real pattern_ids (fetch_known_issues) are numeric result ids as strings;
    # a lexicographic min would wrongly pick "100" over "9" here.
    summaries = [
        {"job_id": "job-1", "status": "analyzed", "confidence": "high"},
        {"job_id": "job-2", "status": "analyzed", "confidence": "high"},
    ]
    known_issues = [
        {"result_id": 9, "recurrence_count": 1, "pattern_id": "9"},
        {"result_id": 100, "recurrence_count": 1, "pattern_id": "100"},
    ]
    output = {
        "historical_matches": [
            {
                "job_id": "job-1",
                "matches": [
                    {
                        "matched_result_id": 9,
                        "similarity_reasoning": "Same failure mode.",
                        "confidence": "high",
                    }
                ],
            },
            {
                "job_id": "job-2",
                "matches": [
                    {
                        "matched_result_id": 100,
                        "similarity_reasoning": "Same failure mode.",
                        "confidence": "high",
                    }
                ],
            },
        ],
        "cross_job_patterns": [
            {
                "pattern": "ambiguous failure",
                "jobs": ["job-1", "job-2"],
                "description": "The jobs share a failure mode.",
                "confidence": "high",
            }
        ],
    }

    normalized = orchestrator._normalize_semantics(output, summaries, known_issues)

    assert normalized["cross_job_patterns"][0]["pattern_id"] == "9"


def test_aggregate_semantics_uses_model_generated_output_schema(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    query_result = orchestrator.SDKQueryResult(
        is_error=False,
        cost_usd=None,
        usage={},
        text="",
        structured_output={"historical_matches": [], "cross_job_patterns": []},
    )
    sdk_query = AsyncMock(return_value=query_result)
    monkeypatch.setattr(orchestrator, "_run_sdk_query", sdk_query)
    config = _config(tmp_path)
    job_summaries = [
        {"job_id": "201", "status": "analyzed", "confidence": "high"},
        {"job_id": "202", "status": "analyzed", "confidence": "high"},
    ]

    cross_patterns, result = asyncio.run(
        orchestrator._aggregate_semantics(
            job_summaries,
            [],
            config,
            cwd=tmp_path,
            semaphore=asyncio.Semaphore(config.max_parallel_jobs),
        )
    )

    expected_schema = BatchSemanticOutput.model_json_schema()
    assert cross_patterns == []
    assert result is query_result
    assert sdk_query.call_args.kwargs["output_format"] == {
        "type": "json_schema",
        "schema": expected_schema,
    }


@pytest.mark.parametrize("failure", ["unavailable", "invalid_output"])
def test_semantic_failure_falls_back_to_deterministic_matching(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str
) -> None:
    artifacts = AnalysisArtifacts(
        job_id="201",
        analysis_dir=tmp_path / "201",
        job_context={"job_name": "RHPDS a.widget.dev-201-create"},
        splunk_logs={},
        correlation={},
        github_fetch_history={},
    )
    execution = orchestrator.JobExecution(
        job_id="201",
        status="completed",
        duration_ms=100,
        artifacts=artifacts,
        summary={
            "catalog_item": "widget",
            "root_cause": {
                "summary": "Worker 7 timed out.",
                "category": "infrastructure",
                "confidence": "high",
            },
        },
    )
    known_issue = {
        "result_id": 9,
        "catalog_item": "widget",
        "root_cause_category": "infrastructure",
        "root_cause_summary": "Worker 7 timed out.",
        "recurrence_count": 2,
        "pattern_id": "9",
    }
    if failure == "unavailable":
        monkeypatch.setattr(
            orchestrator,
            "_aggregate_semantics",
            AsyncMock(side_effect=RuntimeError("semantic unavailable")),
        )
    else:
        monkeypatch.setattr(
            orchestrator,
            "_run_sdk_query",
            AsyncMock(
                return_value=orchestrator.SDKQueryResult(
                    False, None, {}, "", structured_output={"historical_matches": []}
                )
            ),
        )

    summaries, cross_patterns, aggregation_result = orchestrator._run_agent_batch(
        [execution], [known_issue], _config(tmp_path), cwd=tmp_path
    )

    assert cross_patterns == []
    assert aggregation_result is None
    assert summaries[0]["historical_matches"] == [
        {
            "matched_result_id": 9,
            "recurrence_count": 2,
            "similarity_reasoning": "Deterministic root-cause summary similarity is 100%.",
            "pattern_id": "9",
            "confidence": "high",
            "match_source": "deterministic_fallback",
            "evidence": [
                {
                    "statement_type": "observed",
                    "description": "The root-cause summaries are 100% similar.",
                    "source": "deterministic_fallback",
                }
            ],
        }
    ]


def test_deterministic_historical_matches_tolerates_malformed_known_issue() -> None:
    summary = {
        "job_id": "201",
        "status": "analyzed",
        "catalog_item": "widget",
        "root_cause_category": "infrastructure",
        "root_cause_summary": "Worker 7 timed out.",
    }
    malformed_known_issue = {
        "result_id": "not-an-int",
        "catalog_item": "widget",
        "root_cause_category": "infrastructure",
        "root_cause_summary": "Worker 7 timed out.",
    }

    orchestrator._deterministic_historical_matches([summary], [malformed_known_issue])

    assert summary["historical_matches"] == []


def test_empty_semantic_match_falls_back_to_deterministic_matching(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    artifacts = AnalysisArtifacts(
        job_id="201",
        analysis_dir=tmp_path / "201",
        job_context={"job_name": "RHPDS a.widget.dev-201-create"},
        splunk_logs={},
        correlation={},
        github_fetch_history={},
    )
    execution = orchestrator.JobExecution(
        job_id="201",
        status="completed",
        duration_ms=100,
        artifacts=artifacts,
        summary={
            "catalog_item": "widget",
            "root_cause": {
                "summary": "Worker 7 timed out.",
                "category": "infrastructure",
                "confidence": "high",
            },
        },
    )
    known_issue = {
        "result_id": 9,
        "catalog_item": "widget",
        "root_cause_category": "infrastructure",
        "root_cause_summary": "Worker 7 timed out.",
    }
    monkeypatch.setattr(
        orchestrator,
        "_aggregate_semantics",
        AsyncMock(return_value=([], None)),
    )

    summaries, cross_patterns, aggregation_result = orchestrator._run_agent_batch(
        [execution], [known_issue], _config(tmp_path), cwd=tmp_path
    )

    assert cross_patterns == []
    assert aggregation_result is None
    assert summaries[0]["historical_matches"][0]["matched_result_id"] == 9


def test_report_aggregation_is_deterministic(tmp_path: Path) -> None:
    first_artifacts = AnalysisArtifacts(
        job_id="201",
        analysis_dir=tmp_path / "201",
        job_context={"job_name": "RHPDS a.widget.dev-201-create"},
        splunk_logs={},
        correlation={},
        github_fetch_history={},
    )
    second_artifacts = AnalysisArtifacts(
        job_id="202",
        analysis_dir=tmp_path / "202",
        job_context={"job_name": "RHPDS a.widget.dev-202-create"},
        splunk_logs={},
        correlation={},
        github_fetch_history={},
    )
    executions = [
        orchestrator.JobExecution(
            job_id="201",
            status="completed",
            duration_ms=100,
            artifacts=first_artifacts,
            summary={
                "root_cause": {
                    "summary": "Worker capacity exhausted",
                    "category": "infrastructure",
                    "confidence": "high",
                },
                "recommendations": [
                    {"action": "Add worker capacity", "priority": "low", "details": "Scale up"}
                ],
            },
        ),
        orchestrator.JobExecution(
            job_id="202",
            status="completed",
            duration_ms=200,
            artifacts=second_artifacts,
            summary={
                "root_cause": {
                    "summary": "Same worker capacity issue",
                    "category": "infrastructure",
                    "confidence": "medium",
                },
                "recommendations": [
                    {
                        "action": "  ADD worker capacity  ",
                        "priority": "high",
                        "details": "Increase worker pool",
                    }
                ],
            },
        ),
        orchestrator.JobExecution(
            job_id="203",
            status="failed",
            duration_ms=50,
            error="Could not fetch job log",
            stage="analysis_pipeline",
        ),
    ]
    job_summaries = [orchestrator._job_summary(execution) for execution in executions]

    report = orchestrator._build_report(
        batch_id="batch_test",
        requested_count=3,
        executions=executions,
        job_summaries=job_summaries,
        cross_job_patterns=[],
        agent_spawn="2026-10-01T10:00:00Z",
        batch_started=time.monotonic(),
        generated_at=datetime(2026, 10, 1, 10, tzinfo=timezone.utc),
    )

    assert report["total_jobs_requested"] == 3
    assert report["total_jobs_analyzed"] == 2
    assert report["total_jobs_failed"] == 1
    assert report["root_cause_category_breakdown"]["infrastructure"]["job_ids"] == [
        "201",
        "202",
    ]
    assert report["confidence_breakdown"] == {"high": 1, "medium": 1, "low": 0}
    assert report["high_priority_recommendations"] == [
        {
            "priority": "high",
            "action": "Add worker capacity",
            "affects_jobs": ["201", "202"],
            "category": "infrastructure",
            "details": "Increase worker pool",
            "github_path": "",
            "rank": 1,
        }
    ]
    assert report["failed_analyses"] == [
        {
            "job_id": "203",
            "error": "Could not fetch job log",
            "stage": "analysis_pipeline",
        }
    ]
    orchestrator._validate_batch_report(report)


def test_write_report_rejects_invalid_report_before_creating_file(tmp_path: Path) -> None:
    config = _config(tmp_path)

    with pytest.raises(ValueError, match="Batch report failed schema validation"):
        orchestrator._write_report(config, {"batch_id": "batch_invalid"})

    assert not (config.state_dir / "reports").exists()


def test_jira_step_is_an_explicit_no_op(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="rca.batch")

    orchestrator.prepare_jira_tickets({"batch_id": "batch_test"})

    assert "[STEP 6] Preparing Jira tickets" in caplog.text
    assert "no tickets created" in caplog.text


def test_mlflow_logs_sdk_cost_usage_and_per_model_usage(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    metrics: dict[str, float] = {}
    mlflow = ModuleType("mlflow")
    mlflow.set_tracking_uri = Mock()
    mlflow.get_experiment_by_name = Mock(return_value=SimpleNamespace(experiment_id="exp-1"))
    mlflow.start_run = Mock(return_value=nullcontext())
    mlflow.set_tags = Mock()
    mlflow.log_metric = lambda name, value: metrics.__setitem__(name, value)
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)

    config = _config(tmp_path)
    config.environment.update(
        {
            "MLFLOW_TRACKING_URI": "file:///tmp/mlruns",
            "MLFLOW_EXPERIMENT_NAME": "rca-tests",
        }
    )
    executions = [
        orchestrator.JobExecution(
            job_id="42",
            status="completed",
            duration_ms=100,
            cost_usd=0.1,
            usage={"input_tokens": 10, "output_tokens": 4},
            model_usage={
                "claude-test-v1": {
                    "inputTokens": 8,
                    "outputTokens": 4,
                    "costUSD": 0.1,
                }
            },
        )
    ]
    aggregation_result = orchestrator.SDKQueryResult(
        is_error=False,
        cost_usd=0.02,
        usage={"input_tokens": 3},
        text="aggregated",
        model_usage={"claude-test-v1": {"inputTokens": 2, "costUSD": 0.02}},
    )

    orchestrator._log_mlflow_usage(config, "batch-test", executions, aggregation_result)

    assert metrics["job_42_cost_usd"] == pytest.approx(0.1)
    assert metrics["job_42_input_tokens"] == 10
    assert metrics["job_42_model_claude-test-v1_inputTokens"] == 8
    assert metrics["job_42_model_claude-test-v1_costUSD"] == pytest.approx(0.1)
    assert metrics["batch_aggregation_model_claude-test-v1_inputTokens"] == 2
    assert metrics["batch_model_claude-test-v1_inputTokens"] == 10
    assert metrics["batch_model_claude-test-v1_costUSD"] == pytest.approx(0.12)
    assert metrics["batch_input_tokens"] == 13
    assert metrics["batch_cost_usd"] == pytest.approx(0.12)


def test_mlflow_omits_incomplete_batch_cost_but_keeps_tokens_and_known_costs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    metrics: dict[str, float] = {}
    mlflow = ModuleType("mlflow")
    mlflow.set_tracking_uri = Mock()
    mlflow.get_experiment_by_name = Mock(return_value=SimpleNamespace(experiment_id="exp-1"))
    mlflow.start_run = Mock(return_value=nullcontext())
    mlflow.set_tags = Mock()
    mlflow.log_metric = lambda name, value: metrics.__setitem__(name, value)
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)
    config = _config(tmp_path)
    config.environment.update(
        {
            "MLFLOW_TRACKING_URI": "file:///tmp/mlruns",
            "MLFLOW_EXPERIMENT_NAME": "rca-tests",
        }
    )
    executions = [
        orchestrator.JobExecution(
            "42", "completed", 100, cost_usd=0.1,
            usage={"input_tokens": 10, "output_tokens": 4},
        ),
        orchestrator.JobExecution(
            "43", "completed", 100, cost_usd=None,
            usage={"input_tokens": 7, "output_tokens": 3},
        ),
    ]
    aggregation = orchestrator.SDKQueryResult(
        False, 0.02, {"input_tokens": 3}, "aggregated"
    )

    orchestrator._log_mlflow_usage(config, "batch-test", executions, aggregation)

    assert "batch_cost_usd" not in metrics
    assert "job_43_cost_usd" not in metrics
    assert metrics["job_42_cost_usd"] == pytest.approx(0.1)
    assert metrics["batch_aggregation_cost_usd"] == pytest.approx(0.02)
    assert metrics["job_43_input_tokens"] == 7
    assert metrics["batch_aggregation_input_tokens"] == 3
    assert metrics["batch_input_tokens"] == 20
    assert metrics["batch_output_tokens"] == 7


def test_mlflow_tracing_is_opt_in_and_configures_agent_sdk_autolog(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mlflow = ModuleType("mlflow")
    mlflow.set_tracking_uri = Mock()
    mlflow.set_experiment = Mock()
    anthropic = ModuleType("mlflow.anthropic")
    anthropic.autolog = Mock()
    mlflow.anthropic = anthropic
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)
    monkeypatch.setitem(sys.modules, "mlflow.anthropic", anthropic)

    config = _config(tmp_path)
    config.environment.update(
        {
            "MLFLOW_CLAUDE_TRACING_ENABLED": "true",
            "MLFLOW_TRACKING_URI": "https://mlflow.example.test",
            "MLFLOW_EXPERIMENT_NAME": "rca-tests",
        }
    )

    orchestrator._enable_mlflow_tracing(config)

    mlflow.set_tracking_uri.assert_called_once_with("https://mlflow.example.test")
    mlflow.set_experiment.assert_called_once_with("rca-tests")
    anthropic.autolog.assert_called_once_with()


def test_mlflow_tracing_is_skipped_when_disabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mlflow = ModuleType("mlflow")
    mlflow.set_tracking_uri = Mock()
    mlflow.anthropic = ModuleType("mlflow.anthropic")
    mlflow.anthropic.autolog = Mock()
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)

    config = _config(tmp_path)
    config.environment.update(
        {
            "MLFLOW_CLAUDE_TRACING_ENABLED": "false",
            "MLFLOW_TRACKING_URI": "https://mlflow.example.test",
            "MLFLOW_EXPERIMENT_NAME": "rca-tests",
        }
    )

    orchestrator._enable_mlflow_tracing(config)

    mlflow.set_tracking_uri.assert_not_called()
    mlflow.anthropic.autolog.assert_not_called()


def test_batch_disables_pipeline_tracing_without_disabling_agent_tracing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_calls: list[str] = []

    class FakeMLflow:
        def trace(self, name: str, span_type=None):
            def decorator(fn):
                def wrapper(*args: object, **kwargs: object):
                    pipeline_calls.append(name)
                    return fn(*args, **kwargs)

                return wrapper

            return decorator

    monkeypatch.setattr(tracing, "HAS_MLFLOW", True)
    monkeypatch.setattr(tracing, "mlflow", FakeMLflow())
    tracing.set_pipeline_tracing_enabled(True)

    @tracing.trace("Pipeline step")
    def pipeline_step() -> str:
        return "ok"

    tracing.set_pipeline_tracing_enabled(False)
    assert pipeline_step() == "ok"
    assert pipeline_calls == []

    tracing.set_pipeline_tracing_enabled(True)
    assert pipeline_step() == "ok"
    assert pipeline_calls == ["Pipeline step"]
