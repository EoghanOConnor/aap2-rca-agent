"""Preflight checks must test live connectivity, not only that settings exist."""

import requests

from rca.analysis import setup


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


def _splunk_env(monkeypatch):
    monkeypatch.setenv("SPLUNK_HOST", "https://splunk.example.test:8089")
    monkeypatch.setenv("SPLUNK_USERNAME", "user")
    monkeypatch.setenv("SPLUNK_PASSWORD", "pw")
    monkeypatch.delenv("SPLUNK_TOKEN", raising=False)


def test_splunk_ok_when_authenticated(monkeypatch):
    _splunk_env(monkeypatch)
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return FakeResponse(200)

    monkeypatch.setattr(requests, "get", fake_get)
    result = setup.check_splunk()
    assert result["status"] == "ok"
    assert calls[0][0].endswith("/services/server/info")
    assert calls[0][1]["auth"] == ("user", "pw")


def test_splunk_token_uses_bearer_header(monkeypatch):
    monkeypatch.setenv("SPLUNK_HOST", "https://splunk.example.test:8089")
    monkeypatch.delenv("SPLUNK_USERNAME", raising=False)
    monkeypatch.delenv("SPLUNK_PASSWORD", raising=False)
    monkeypatch.setenv("SPLUNK_TOKEN", "tok")
    seen = {}
    monkeypatch.setattr(
        requests, "get", lambda url, **kw: seen.update(kw) or FakeResponse(200)
    )
    assert setup.check_splunk()["status"] == "ok"
    assert seen["headers"]["Authorization"] == "Bearer tok"


def test_splunk_rejected_credentials_is_error(monkeypatch):
    _splunk_env(monkeypatch)
    monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(401))
    result = setup.check_splunk()
    assert result["status"] == "error"
    assert "401" in result["message"]


def test_splunk_unreachable_is_error(monkeypatch):
    _splunk_env(monkeypatch)

    def boom(*a, **k):
        raise requests.ConnectionError("down")

    monkeypatch.setattr(requests, "get", boom)
    result = setup.check_splunk()
    assert result["status"] == "error"
    assert "connection failed" in result["message"]


def test_github_ok_reports_login(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_x")
    monkeypatch.setattr(
        requests, "get", lambda *a, **k: FakeResponse(200, {"login": "octocat"})
    )
    result = setup.check_github()
    assert result["status"] == "ok"
    assert "octocat" in result["message"]


def test_github_bad_token_is_error(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_bad")
    monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(401))
    assert setup.check_github()["status"] == "error"


def test_missing_config_does_not_probe(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "<token>")
    monkeypatch.delenv("SPLUNK_HOST", raising=False)

    def fail(*a, **k):
        raise AssertionError("must not make a network call")

    monkeypatch.setattr(requests, "get", fail)
    assert setup.check_github()["status"] == "missing"
    assert setup.check_splunk()["status"] == "missing"
