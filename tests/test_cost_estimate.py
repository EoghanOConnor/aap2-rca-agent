import pytest

from rca.analysis import cost_estimate as pce
from rca.analysis import cli


@pytest.mark.parametrize(
    ("n", "days", "expected"),
    [
        (12, 7, "high"),
        (10, 7, "high"),
        (9, 7, "medium"),
        (3, 7, "medium"),
        (2, 7, "low"),
        (50, 14, "low"),
        (4, 30, "low"),
    ],
)
def test_confidence(n, days, expected):
    assert pce.confidence(n, days) == expected


class FakeMCP:
    def __init__(self):
        self.sql = []

    def query(self, sql):
        self.sql.append(sql)
        return [{"n": 0, "median_per_hr": None, "min_per_hr": None, "max_per_hr": None}]


@pytest.mark.parametrize("days", [7, 14, 30])
def test_month_ts_bound_scales_with_window(days):
    mcp = FakeMCP()
    prov = {"uuid": "abc-123", "catalog_id": 42, "provisioned_at": "2026-10-29 19:33:17"}
    pce.comparison_stats(mcp, prov, days)
    sql = mcp.sql[0]
    assert f"interval '{days + 31} days'" in sql
    assert f"interval '{days} days'" in sql
    assert "p.catalog_id = 42" in sql


def test_iqr_filter_drops_outlier():
    rows = [{"v": float(x)} for x in (1, 2, 3, 4, 5, 6, 7, 8, 9, 100)]
    kept = [r["v"] for r in pce.iqr_filter(rows, "v")]
    assert 100.0 not in kept and len(kept) == 9


def test_iqr_filter_keeps_all_when_no_spread():
    rows = [{"v": 5.0}] * 6
    assert pce.iqr_filter(rows, "v") == rows


def test_baseline_median_ignores_cost_outlier():
    rows = [{"usage_amount": float(u), "lifetime_hours": 10.0} for u in (9, 10, 11, 12, 9, 10, 11, 12, 10)]
    rows.append({"usage_amount": 5000.0, "lifetime_hours": 10.0})
    out = pce.baseline_from_rows(rows)
    assert out["median_per_hr"] == 1.0
    assert out["n"] == 9


def test_baseline_floors_hours_and_handles_empty():
    out = pce.baseline_from_rows([{"usage_amount": "3", "lifetime_hours": "0.5"}])
    assert out["median_per_hr"] == 3.0
    assert pce.baseline_from_rows([]) is None


def test_catalog_baseline_sql_filters():
    class Capture(FakeMCP):
        def query(self, sql):
            self.sql.append(sql)
            return [{"uuid": "u", "usage_amount": "4", "lifetime_hours": "2"}]

    mcp = Capture()
    prov = {"uuid": "abc-123", "catalog_id": 42, "provisioned_at": "2026-10-29 19:33:17"}
    assert pce.catalog_baseline(mcp, prov)["median_per_hr"] == 2.0
    sql = mcp.sql[0]
    assert "pmv.environment = 'PROD'" in sql and "pmv.provision_result = 'success'" in sql
    assert "IS DISTINCT FROM" in sql and "ps.catalog_id = 42" in sql
    assert "ci.asset_uuid = (SELECT asset_uuid FROM catalog_items WHERE id = 42)" in sql
    assert "pmv.provision_time AS provision_hours" in sql
    assert f"LIMIT {pce.BASELINE_ROWS}" in sql

    mcp2 = Capture()
    pce.catalog_baseline(mcp2, {**prov, "environment": "EVENT"})
    assert "pmv.environment = 'EVENT'" in mcp2.sql[0]
    with pytest.raises(ValueError):
        pce.catalog_baseline(Capture(), {**prov, "environment": "x'; --"})


def test_catalog_baseline_filters_only_prod_and_event():
    class Capture(FakeMCP):
        def query(self, sql):
            self.sql.append(sql)
            return []

    prov = {"uuid": "abc-123", "catalog_id": 42, "provisioned_at": "2026-10-29 19:33:17"}
    dev = Capture()
    pce.catalog_baseline(dev, {**prov, "environment": "DEV"})
    assert "pmv.provision_result" not in dev.sql[0] and "IS DISTINCT FROM" not in dev.sql[0]
    prod = Capture()
    pce.catalog_baseline(prod, {**prov, "environment": "PROD"})
    assert "pmv.provision_result = 'success'" in prod.sql[0]


def test_baseline_applies_iqr_to_runtime_and_provision_hours():
    def row(runtime, provision):
        return {"usage_amount": 10.0, "lifetime_hours": 10.0,
                "runtime_hours": runtime, "provision_hours": provision}

    rows = [row(float(h), 1.0 + i * 0.1) for i, h in enumerate((8, 9, 10, 11, 12, 8, 9, 10, 11))]
    rows.append(row(900.0, 1.0))  # runtime outlier
    rows.append(row(10.0, 500.0))  # provision-time outlier
    assert pce.baseline_from_rows(rows)["n"] == 9


def test_baseline_drops_rows_with_null_provision_hours_like_the_service():
    rows = [{"usage_amount": float(u), "lifetime_hours": 10.0, "runtime_hours": 10.0,
             "provision_hours": h}
            for u, h in ((10, 1.0), (11, 2.0), (12, 3.0), (13, 4.0), (14, None))]
    assert pce.baseline_from_rows(rows)["n"] == 4


def test_comparison_stats_rejects_odd_timestamp():
    prov = {"uuid": "abc", "catalog_id": 1, "provisioned_at": "'; DROP TABLE x;--"}
    with pytest.raises(ValueError):
        pce.comparison_stats(FakeMCP(), prov, 7)


def test_provision_uuid_from_job_name():
    name = "RHPDS openshift-cnv.x.dev-sbss5-1-provision-zd4jw 574A0E9C-9845-5db0-94db-b7362caa394d"
    assert pce.provision_uuid_from_job_name(name) == "574a0e9c-9845-5db0-94db-b7362caa394d"
    assert pce.provision_uuid_from_job_name("no uuid here") is None
    assert pce.provision_uuid_from_job_name("") is None


class ScriptedMCP:
    """Answers fetch_provision, then comparison_stats, from fixed rows."""

    def __init__(self, stats):
        self.stats = stats

    def query(self, sql):
        if "FROM provisions p" in sql and "p.uuid = " in sql and "hours_failing" in sql:
            return [{"uuid": "abc-123", "catalog_id": 42, "catalog_name": "cat",
                     "cloud": "cnv", "last_state": "failed", "provision_result": "failure",
                     "environment": "DEV", "provisioned_at": "2026-10-29 19:33:17",
                     "retired_at": None, "hours_failing": "10"}]
        return [self.stats]


def test_estimate_uses_median_times_hours():
    mcp = ScriptedMCP({"n": 12, "median_per_hr": "2", "min_per_hr": "1", "max_per_hr": "4"})
    out = pce.estimate(mcp, "abc12345-1234")
    est = out["estimate"]
    assert (est["cost_mid"], est["cost_low"], est["cost_high"]) == (20.0, 10.0, 40.0)
    assert est["confidence"] == "high" and est["source"] == "comparison"
    assert "ESTIMATED COST: $20.00" in pce.render(out)


def test_estimate_without_data_reports_no_estimate():
    class NoData(ScriptedMCP):
        def query(self, sql):
            if "hours_failing" in sql:
                return super().query(sql)
            if "pmv" in sql:  # catalog baseline query: no rows
                return []
            return [{"n": 0, "median_per_hr": None, "min_per_hr": None, "max_per_hr": None}]

    out = pce.estimate(NoData({}), "abc12345-1234")
    assert out["estimate"] is None
    assert "No estimate possible" in pce.render(out)


def test_estimate_rejects_bad_uuid_and_missing_provision():
    with pytest.raises(pce.CostEstimateError):
        pce.estimate(ScriptedMCP({}), "x'; DROP TABLE y;--")

    class Empty:
        def query(self, sql):
            return []

    with pytest.raises(pce.CostEstimateError, match="not found"):
        pce.estimate(Empty(), "abc12345-1234")


def test_connect_requires_token():
    with pytest.raises(pce.CostEstimateError, match="REPORTING_MCP_TOKEN"):
        pce.connect({})


def test_cost_command_uses_uuid_from_step1_and_saves(monkeypatch, tmp_path, capsys):
    from types import SimpleNamespace

    job_dir = tmp_path / "77"
    job_dir.mkdir()
    (job_dir / "step1_job_context.json").write_text(
        '{"job_name": "RHPDS x 574a0e9c-9845-5db0-94db-b7362caa394d"}'
    )
    seen = {}
    monkeypatch.setattr(pce, "connect", lambda env: object())
    monkeypatch.setattr(
        pce, "estimate",
        lambda mcp, uuid, include_baseline=False: seen.update(uuid=uuid) or {
            "provision": {"uuid": uuid, "catalog_name": "c", "catalog_id": 1, "cloud": "x",
                          "last_state": "failed", "provision_result": "failure",
                          "provisioned_at": "t"},
            "hours_failing": 1.0, "hours_failing_basis": "proxy", "window_days": 7,
            "estimate": None, "note": "No estimate possible: test", "disclaimer": "d"},
    )
    config = SimpleNamespace(analysis_dir=tmp_path, environment={})
    args = SimpleNamespace(job_id="77", provision_uuid=None, baseline=False)
    assert cli.cmd_cost(args, config) == 0
    assert seen["uuid"] == "574a0e9c-9845-5db0-94db-b7362caa394d"
    assert (job_dir / "step4c_cost_estimate.json").is_file()


def test_cost_command_without_uuid_fails(tmp_path):
    from types import SimpleNamespace

    (tmp_path / "5").mkdir()
    (tmp_path / "5" / "step1_job_context.json").write_text('{"job_name": "no uuid"}')
    config = SimpleNamespace(analysis_dir=tmp_path, environment={})
    assert cli.cmd_cost(SimpleNamespace(job_id="5", provision_uuid=None, baseline=False), config) == 1
