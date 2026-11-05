import importlib.util
from pathlib import Path

import pytest

SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "skills/provision-cost-estimate/scripts/catalog_metrics.py"
)
spec = importlib.util.spec_from_file_location("catalog_metrics", SCRIPT)
pce = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pce)


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
