"""Tests for CLI setup discovery and post-command integrations."""

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from rca.analysis import cli
from rca.analysis.setup import find_repo_root


def test_find_repo_root_from_nested_skill_directory(tmp_path: Path) -> None:
    repo_root = tmp_path / "checkout"
    (repo_root / ".git").mkdir(parents=True)
    skill_dir = repo_root / "skills" / "root-cause-analysis"
    skill_dir.mkdir(parents=True)

    assert find_repo_root(skill_dir) == repo_root


def test_setup_uses_discovered_repo_root(monkeypatch, tmp_path: Path, capsys) -> None:
    repo_root = tmp_path / "checkout"
    repo_root.mkdir()
    (repo_root / "pyproject.toml").touch()
    skill_dir = repo_root / "skills" / "root-cause-analysis"
    skill_dir.mkdir(parents=True)
    monkeypatch.chdir(skill_dir)

    config = SimpleNamespace(environment={})
    monkeypatch.setattr(cli.Config, "from_env", classmethod(lambda cls: config))
    captured: dict[str, Path] = {}

    def fake_run_checks(base_dir: Path, root: Path) -> list[dict[str, str]]:
        captured["root"] = root
        return [{"status": "ok"}]

    monkeypatch.setattr(cli, "run_checks", fake_run_checks)
    monkeypatch.setattr(cli, "_run_mlflow_autolog", lambda *_: None)

    assert cli.main(["setup", "--json"]) == 0
    assert captured["root"] == repo_root
    assert '"status": "ok"' in capsys.readouterr().out


def test_main_runs_mlflow_autolog_after_dispatch(monkeypatch, tmp_path: Path) -> None:
    environment = {
        "MLFLOW_TRACKING_URI": "https://mlflow.example.test",
        "MLFLOW_EXPERIMENT_NAME": "RCA tests",
    }
    config = SimpleNamespace(environment=environment)
    monkeypatch.setattr(cli.Config, "from_env", classmethod(lambda cls: config))
    monkeypatch.setattr(cli, "cmd_parse", lambda args: 7)
    monkeypatch.setattr(cli, "_project_root", lambda: tmp_path)
    calls: list[tuple[Path, dict[str, str]]] = []
    monkeypatch.setattr(
        cli,
        "_run_mlflow_autolog",
        lambda root, env: calls.append((root, env)),
    )

    assert cli.main(["parse", "--job-log", "job.json"]) == 7
    assert calls == [(tmp_path, environment)]


def test_mlflow_autolog_uses_loaded_settings(monkeypatch, tmp_path: Path) -> None:
    environment = {
        "MLFLOW_TRACKING_URI": "https://mlflow.example.test",
        "MLFLOW_EXPERIMENT_NAME": "RCA tests",
        "MLFLOW_TRACKING_TOKEN": "test-token",
    }
    captured: dict[str, object] = {}

    def fake_run(command: list[str], **kwargs: object) -> SimpleNamespace:
        captured["command"] = command
        captured.update(kwargs)
        return SimpleNamespace(returncode=0, stdout="registered", stderr="")

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    cli._run_mlflow_autolog(tmp_path, environment)

    assert captured["command"] == [
        "mlflow",
        "autolog",
        "claude",
        "-u",
        "https://mlflow.example.test",
        "-n",
        "RCA tests",
    ]
    assert captured["cwd"] == str(tmp_path)
    assert captured["env"] == environment
    assert captured["timeout"] == 10


def test_upload_uses_configured_jumpbox_alias(monkeypatch, tmp_path: Path) -> None:
    config = SimpleNamespace(
        analysis_dir=tmp_path / ".analysis",
        jumpbox_uri="agent@unresolvable.example.test",
        environment={"SSH_JUMPBOX_ALIAS": "ci-jumpbox"},
    )
    upload = Mock(return_value=True)
    monkeypatch.setattr(cli, "upload_to_jumpbox", upload)

    assert cli.cmd_upload(Namespace(job_id="123"), config) == 0
    upload.assert_called_once_with(
        "123",
        tmp_path / ".analysis" / "123",
        "agent@unresolvable.example.test",
        "unknown",
        ssh_jumpbox_alias="ci-jumpbox",
    )


def test_setup_checks_reporting_mcp_token(monkeypatch) -> None:
    from rca.analysis.setup import check_reporting_mcp

    monkeypatch.delenv("REPORTING_MCP_TOKEN", raising=False)
    missing = check_reporting_mcp()
    assert missing["status"] == "skipped"
    assert missing["env_vars"][0]["name"] == "REPORTING_MCP_TOKEN"

    monkeypatch.setenv("REPORTING_MCP_TOKEN", "<your-mcp-access-token>")
    assert check_reporting_mcp()["status"] == "skipped"

    monkeypatch.setenv("REPORTING_MCP_TOKEN", "real-token")
    assert check_reporting_mcp()["status"] == "ok"
