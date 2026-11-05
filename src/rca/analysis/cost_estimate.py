#!/usr/bin/env python3
"""Estimate what a failing provision costs, from similar successful provisions.

Deterministic: every number comes from SQL run through the reporting-db-prod MCP
server over HTTP; nothing here is left to model judgement.

    estimated_cost = median(cost per hour of similar provisions) x hours_failing

"Similar" = same catalog_id, provision_result = 'success', retired in the window
before the failing provision started (T0). The window widens 7 -> 14 -> 30 days
until at least MIN_N provisions match. If none match, it falls back to a catalog
baseline (median cost per lifetime hour of the catalog item's recent provisions
with outliers removed, following demo-reporting's CatalogMetricsService).

Environment: REPORTING_MCP_TOKEN (required), REPORTING_MCP_URL (optional).

    rca-analyze cost --job-id <id> [--provision-uuid <uuid>] [--baseline]
"""
import argparse
import json
import os
import re
import statistics
import sys
import urllib.request
from collections.abc import Mapping
from typing import Any

DEFAULT_URL = "https://reporting-mcp.apps.ocpv-infra01.dal12.infra.demo.redhat.com/mcp/"
WINDOWS_DAYS = (7, 14, 30)
MIN_N = 3
# Catalog baseline, adapted from demo-reporting
# reporting-api/src/app/catalog_item/services/catalog_metrics.py
# (commit f084048b: CatalogMetricsService). Ported rules:
#   - success-only and infra-user exclusion apply only to FILTERED_ENVIRONMENTS (PROD, EVENT);
#   - provisions are grouped by catalog_items.asset_uuid, not catalog_id;
#   - outliers are removed with a 1.5 x IQR filter on usage_amount, runtime_hours,
#     lifetime_hours and provision_hours, in that order;
#   - the rate is the median of usage_amount / lifetime_hours.
# Deliberate differences: provisions with no cost rows are skipped (the service counts them as
# zero), and only the latest BASELINE_ROWS provisions in BASELINE_LOOKBACK_DAYS are sampled
# because the MCP gateway caps a query at 500 rows.
FILTERED_ENVIRONMENTS = {"PROD", "EVENT"}
BASELINE_LOOKBACK_DAYS = 180
BASELINE_ROWS = 400  # the MCP gateway returns at most 500 rows per query
EXCLUDED_EMAIL = "rhpds-opentlc-infra+rhpds-user@redhat.com"
UUID_RE = re.compile(r"^[0-9a-fA-F-]{8,64}$")


class MCP:
    def __init__(self, url, token):
        self.url, self.token, self.session, self._id = url, token, None, 0
        self._rpc("initialize", {
            "protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "rca-cost-estimate", "version": "1"},
        })
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def _post(self, payload):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": f"Bearer {self.token}",
        }
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        req = urllib.request.Request(self.url, json.dumps(payload).encode(), headers)
        with urllib.request.urlopen(req, timeout=120) as resp:
            self.session = resp.headers.get("Mcp-Session-Id", self.session)
            body = resp.read().decode()
            ctype = resp.headers.get("Content-Type", "")
        if not body.strip():
            return None
        if "text/event-stream" in ctype:
            data = [ln[5:].strip() for ln in body.splitlines() if ln.startswith("data:")]
            return json.loads(data[-1])
        return json.loads(body)

    def _rpc(self, method, params):
        self._id += 1
        msg = self._post({"jsonrpc": "2.0", "id": self._id, "method": method, "params": params})
        if "error" in msg:
            raise RuntimeError(msg["error"])
        return msg["result"]

    def query(self, sql):
        result = self._rpc("tools/call", {
            "name": "query", "arguments": {"sql": sql, "output_format": "json"},
        })
        text = "".join(c.get("text", "") for c in result.get("content", []))
        if result.get("isError"):
            raise RuntimeError(text)
        data = json.loads(text)
        if data.get("truncated"):
            raise RuntimeError("query result truncated")
        return data["rows"]


def fetch_provision(mcp, uuid):
    # Widen the month_ts lower bound once if the first lookup misses.
    for months in (6, 24):
        rows = mcp.query(f"""
            SELECT p.uuid, p.catalog_id, p.provision_result, p.last_state, p.cloud, p.environment,
                   p.provisioned_at, p.retired_at, ci.display_name AS catalog_name,
                   ROUND(EXTRACT(epoch FROM COALESCE(p.retired_at, now()) - p.provisioned_at) / 3600, 2) AS hours_failing
            FROM provisions p
            LEFT JOIN catalog_items ci ON ci.id = p.catalog_id
            WHERE p.uuid = '{uuid}'
              AND p.month_ts >= DATE_TRUNC('month', now() - interval '{months} months')
        """)
        if rows:
            return rows[0]
    return None


def comparison_stats(mcp, prov, days):
    t0 = str(prov["provisioned_at"])
    if not re.match(r"^\d{4}-\d{2}-\d{2}[ T][\d:.+-]+$", t0):
        raise ValueError(f"unexpected provisioned_at: {t0!r}")
    # month_ts is the month a provision *started*, so reach back one more month
    # than the window (the skill's fixed 37 days is too tight beyond 7 days).
    lower = f"DATE_TRUNC('month', TIMESTAMP '{t0}' - interval '{days + 31} days')"
    upper = f"DATE_TRUNC('month', TIMESTAMP '{t0}')"
    rows = mcp.query(f"""
        WITH cmp AS (
          SELECT p.uuid,
                 GREATEST(EXTRACT(epoch FROM p.retired_at - p.provisioned_at) / 3600, 1) AS hrs
          FROM provisions p
          WHERE p.month_ts >= {lower} AND p.month_ts <= {upper}
            AND p.catalog_id = {int(prov['catalog_id'])}
            AND p.uuid <> '{prov['uuid']}'
            AND p.provision_result = 'success'
            AND p.retired_at >= TIMESTAMP '{t0}' - interval '{days} days'
            AND p.retired_at <  TIMESTAMP '{t0}'
        ), cost AS (
          SELECT pc.provision_uuid, SUM(pc.total_cost) AS total_cost
          FROM provision_cost pc
          WHERE pc.month_ts >= {lower} AND pc.month_ts <= {upper}
            AND pc.provision_uuid IN (SELECT uuid FROM cmp)
          GROUP BY pc.provision_uuid
        )
        SELECT COUNT(*) AS n,
               ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY c.total_cost / cmp.hrs)::numeric, 4) AS median_per_hr,
               ROUND(MIN(c.total_cost / cmp.hrs)::numeric, 4) AS min_per_hr,
               ROUND(MAX(c.total_cost / cmp.hrs)::numeric, 4) AS max_per_hr
        FROM cmp JOIN cost c ON c.provision_uuid = cmp.uuid
    """)
    return rows[0]


IQR_COLUMNS = ("usage_amount", "runtime_hours", "lifetime_hours", "provision_hours")


def iqr_filter(rows, key):
    """Drop rows whose `key` is outside 1.5 x IQR, as the catalog metrics service does.

    Mirrors remove_outliers_iqr: a missing column or empty input is returned unchanged, an IQR
    of 0 keeps every row, and None values (NaN in the service) are ignored when computing the
    quartiles but fail the range test, so they are dropped whenever the filter is applied.
    """
    if not rows or key not in rows[0]:
        return rows
    values = [r[key] for r in rows if r[key] is not None]
    if not values:
        return []
    if len(values) < 2:
        return rows
    q1, _, q3 = statistics.quantiles(values, n=4, method="inclusive")
    iqr = q3 - q1
    if iqr == 0:
        return rows
    return [r for r in rows if r[key] is not None and q1 - 1.5 * iqr <= r[key] <= q3 + 1.5 * iqr]


def _num(value):
    return None if value is None else float(value)


def baseline_from_rows(rows):
    """Median cost per lifetime hour after outlier removal, or None if nothing is left."""
    cleaned = []
    for r in rows:
        row = {
            "usage_amount": float(r["usage_amount"]),
            "lifetime_hours": max(float(r["lifetime_hours"]), 1.0),
        }
        if "runtime_hours" in r:
            row["runtime_hours"] = max(_num(r["runtime_hours"]) or 1.0, 1.0)
        if "provision_hours" in r:
            row["provision_hours"] = _num(r["provision_hours"])
        cleaned.append(row)
    for column in IQR_COLUMNS:
        cleaned = iqr_filter(cleaned, column)
    if not cleaned:
        return None
    rates = [r["usage_amount"] / r["lifetime_hours"] for r in cleaned]
    return {
        "n": len(rates),
        "median_per_hr": round(statistics.median(rates), 4),
        "min_per_hr": round(min(rates), 4),
        "max_per_hr": round(max(rates), 4),
    }


def catalog_baseline(mcp, prov):
    """Catalog-level rate from the latest provisions of the asset before T0, any window.

    Used when the T0-anchored comparison finds nothing, or on request as a cross-check.
    Unlike the metrics service, provisions with no cost rows are skipped rather than
    counted as zero cost, which would drag the median down.
    """
    t0 = str(prov["provisioned_at"])
    if not re.match(r"^\d{4}-\d{2}-\d{2}[ T][\d:.+-]+$", t0):
        raise ValueError(f"unexpected provisioned_at: {t0!r}")
    # Same environment as the failing provision, as the metrics service groups by environment.
    env = prov.get("environment") or "PROD"
    if not re.match(r"^[A-Za-z0-9_-]+$", env):
        raise ValueError(f"unexpected environment: {env!r}")
    lower = f"DATE_TRUNC('month', TIMESTAMP '{t0}' - interval '{BASELINE_LOOKBACK_DAYS + 31} days')"
    upper = f"DATE_TRUNC('month', TIMESTAMP '{t0}')"
    # The service filters success / infra user only for PROD and EVENT.
    env_filters = ""
    if env in FILTERED_ENVIRONMENTS:
        env_filters = (
            "AND pmv.provision_result = 'success' "
            f"AND pmv.user_email IS DISTINCT FROM '{EXCLUDED_EMAIL}'"
        )
    catalog_id = int(prov["catalog_id"])
    rows = mcp.query(f"""
        WITH sel AS (
          SELECT ps.uuid,
                 CASE WHEN ps.lifetime_hours < 1 THEN 1 ELSE ps.lifetime_hours END AS lifetime_hours,
                 GREATEST(CASE WHEN ps.runtime_hours > 0 THEN ps.runtime_hours
                               ELSE ps.lifetime_hours END, 1) AS runtime_hours,
                 pmv.provision_time AS provision_hours
          FROM provisions_summary ps
          JOIN provisions_mat_v pmv ON pmv.uuid = ps.uuid
          JOIN catalog_items ci ON ci.id = ps.catalog_id
          WHERE (ps.catalog_id = {catalog_id}
                 OR (ci.asset_uuid IS NOT NULL
                     AND ci.asset_uuid = (SELECT asset_uuid FROM catalog_items WHERE id = {catalog_id})))
            AND ps.uuid <> '{prov['uuid']}'
            AND ps.provisioned_at <  TIMESTAMP '{t0}'
            AND ps.provisioned_at >= TIMESTAMP '{t0}' - interval '{BASELINE_LOOKBACK_DAYS} days'
            AND pmv.environment = '{env}'
            {env_filters}
          ORDER BY ps.provisioned_at DESC
          LIMIT {BASELINE_ROWS}
        )
        SELECT sel.uuid, sel.lifetime_hours, sel.runtime_hours, sel.provision_hours,
               SUM(pc.total_cost) AS usage_amount
        FROM sel JOIN provision_cost pc ON pc.provision_uuid = sel.uuid
        WHERE pc.month_ts >= {lower} AND pc.month_ts <= {upper}
        GROUP BY sel.uuid, sel.lifetime_hours, sel.runtime_hours, sel.provision_hours
    """)
    return baseline_from_rows(rows)


def confidence(n, days):
    if n < MIN_N or days > 7:
        return "low"
    return "high" if n >= 10 else "medium"


JOB_NAME_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)


class CostEstimateError(Exception):
    """Raised when no estimate can be attempted (missing token, bad UUID, not found)."""


def provision_uuid_from_job_name(job_name: str) -> str | None:
    """Return the UUID that AAP appends to the job name, if there is one."""
    match = JOB_NAME_UUID_RE.search(job_name or "")
    return match.group(0).lower() if match else None


def estimate(mcp, uuid: str, *, include_baseline: bool = False) -> dict[str, Any]:
    """Run the full method and return a structured result (no printing)."""
    if not UUID_RE.match(uuid):
        raise CostEstimateError(f"not a valid provision UUID: {uuid!r}")
    prov = fetch_provision(mcp, uuid)
    if not prov:
        raise CostEstimateError(f"Provision {uuid} not found.")

    stats, used = None, None
    for days in WINDOWS_DAYS:
        stats, used = comparison_stats(mcp, prov, days), days
        if stats["n"] >= MIN_N:
            break

    baseline = catalog_baseline(mcp, prov) if (include_baseline or not stats["n"]) else None
    result: dict[str, Any] = {
        "provision": {k: prov.get(k) for k in (
            "uuid", "catalog_id", "catalog_name", "cloud", "last_state",
            "provision_result", "environment", "provisioned_at", "retired_at",
        )},
        "hours_failing": float(prov["hours_failing"]),
        "hours_failing_basis": "provisioned_at is a proxy for failure start",
        "window_days": used,
        "baseline": baseline,
        "estimate": None,
        "disclaimer": "Estimate, not billed cost; cost data can lag a day or more.",
    }
    if stats["n"]:
        source, rates, conf = "comparison", stats, confidence(stats["n"], used)
    elif baseline:
        source, rates, conf = "catalog_baseline", baseline, "low"
    else:
        result["note"] = (
            f"No estimate possible: no successful comparison provisions in {used} days "
            "before T0, and no catalog baseline."
        )
        return result

    med, lo, hi = (float(rates[k]) for k in ("median_per_hr", "min_per_hr", "max_per_hr"))
    hrs = result["hours_failing"]
    result["estimate"] = {
        "source": source,
        "n": int(rates["n"]),
        "confidence": conf,
        "median_per_hr": med,
        "min_per_hr": lo,
        "max_per_hr": hi,
        "cost_mid": round(med * hrs, 2),
        "cost_low": round(lo * hrs, 2),
        "cost_high": round(hi * hrs, 2),
        "skewed": med == hi,
    }
    return result


def render(result: Mapping[str, Any]) -> str:
    """Format an ``estimate`` result as the human-readable report."""
    prov = result["provision"]
    lines = [
        f"Provision:  {prov['uuid']}",
        f"Catalog:    {prov['catalog_name']} (id {prov['catalog_id']})",
        f"Cloud/state: {prov['cloud']} / {prov['last_state']} (result: {prov['provision_result']}, "
        f"environment: {prov.get('environment') or 'unknown'})",
        f"Failing since {prov['provisioned_at']}: {result['hours_failing']} h "
        f"({result['hours_failing_basis']})",
    ]
    est = result["estimate"]
    if not est:
        lines += ["", result["note"]]
        return "\n".join(lines)
    if est["source"] == "comparison":
        lines.append(f"Comparison: {est['n']} provisions, {result['window_days']} days before T0")
    else:
        lines.append(
            f"Comparison: none in {result['window_days']} days before T0; using the catalog "
            f"baseline ({est['n']} recent provisions, outliers removed)"
        )
    lines.append(
        f"Cost/hour:  median {est['median_per_hr']} (min {est['min_per_hr']} - max {est['max_per_hr']})"
    )
    base = result.get("baseline")
    if base and est["source"] == "comparison":
        lines.append(
            f"Cross-check: catalog baseline median {base['median_per_hr']} "
            f"(n={base['n']}, min {base['min_per_hr']} - max {base['max_per_hr']})"
        )
    lines += [
        "",
        f"ESTIMATED COST: ${est['cost_mid']:,.2f}  "
        f"(range ${est['cost_low']:,.2f} - ${est['cost_high']:,.2f})",
        f"Confidence: {est['confidence']}",
    ]
    if est["skewed"]:
        lines.append("Note: median equals max; cost per hour is skewed.")
    lines.append(result["disclaimer"])
    return "\n".join(lines)


def connect(environment: Mapping[str, str]) -> MCP:
    token = environment.get("REPORTING_MCP_TOKEN")
    if not token:
        raise CostEstimateError("set REPORTING_MCP_TOKEN")
    return MCP(environment.get("REPORTING_MCP_URL") or DEFAULT_URL, token)
