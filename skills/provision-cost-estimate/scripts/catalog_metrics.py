#!/usr/bin/env python3
"""Estimate the cost of a failing provision from similar successful ones.

Same method as the provision-cost-estimate skill:
    estimated_cost = median(cost per hour of similar provisions) x hours_failing
where "similar" = same catalog_id, provision_result = 'success', retired in the
window before the failing provision started (T0). The window widens 7 -> 14 -> 30
days until at least MIN_N provisions match.

If no comparison provisions are found even at 30 days, it falls back to a catalog
baseline: the median cost per lifetime hour of the catalog item's recent PROD
successes with outliers removed (the method used by demo-reporting's catalog metrics).
`--baseline` also prints that value as a cross-check. The estimate always uses the median.

Queries go to the reporting-db-prod MCP server over HTTP. See env.example.

    export REPORTING_MCP_TOKEN=...            # required
    export REPORTING_MCP_URL=...              # optional, defaults below
    python3 catalog_metrics.py [--baseline] <provision-uuid>
"""
import argparse
import json
import os
import re
import statistics
import sys
import urllib.request

DEFAULT_URL = "https://reporting-mcp.apps.ocpv-infra01.dal12.infra.demo.redhat.com/mcp/"
WINDOWS_DAYS = (7, 14, 30)
MIN_N = 3
# Catalog baseline, adapted from demo-reporting
# reporting-api/src/app/catalog_item/services/catalog_metrics.py
# (PROD, success only, infra user excluded, IQR outliers removed, median of cost / lifetime hours).
BASELINE_LOOKBACK_DAYS = 180
BASELINE_ROWS = 400  # the MCP gateway returns at most 500 rows per query
EXCLUDED_EMAIL = "rhpds-opentlc-infra+rhpds-user@redhat.com"
UUID_RE = re.compile(r"^[0-9a-fA-F-]{8,64}$")


class MCP:
    def __init__(self, url, token):
        self.url, self.token, self.session, self._id = url, token, None, 0
        self._rpc("initialize", {
            "protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "provision-cost-estimate", "version": "1"},
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


def iqr_filter(rows, key):
    """Drop rows whose `key` is outside 1.5 x IQR, as the catalog metrics service does."""
    if len(rows) < 2:
        return rows
    q1, _, q3 = statistics.quantiles([r[key] for r in rows], n=4, method="inclusive")
    iqr = q3 - q1
    if iqr == 0:
        return rows
    return [r for r in rows if q1 - 1.5 * iqr <= r[key] <= q3 + 1.5 * iqr]


def baseline_from_rows(rows):
    """Median cost per lifetime hour after outlier removal, or None if nothing is left."""
    rows = [
        {"usage_amount": float(r["usage_amount"]), "lifetime_hours": max(float(r["lifetime_hours"]), 1.0)}
        for r in rows
    ]
    rows = iqr_filter(rows, "usage_amount")
    rows = iqr_filter(rows, "lifetime_hours")
    if not rows:
        return None
    rates = [r["usage_amount"] / r["lifetime_hours"] for r in rows]
    return {
        "n": len(rates),
        "median_per_hr": round(statistics.median(rates), 4),
        "min_per_hr": round(min(rates), 4),
        "max_per_hr": round(max(rates), 4),
    }


def catalog_baseline(mcp, prov):
    """Catalog-level rate from the latest PROD successes before T0, any window.

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
    rows = mcp.query(f"""
        WITH sel AS (
          SELECT ps.uuid, GREATEST(ps.lifetime_hours, 1) AS lifetime_hours
          FROM provisions_summary ps
          JOIN provisions_mat_v pmv ON pmv.uuid = ps.uuid
          WHERE ps.catalog_id = {int(prov['catalog_id'])}
            AND ps.uuid <> '{prov['uuid']}'
            AND ps.provisioned_at <  TIMESTAMP '{t0}'
            AND ps.provisioned_at >= TIMESTAMP '{t0}' - interval '{BASELINE_LOOKBACK_DAYS} days'
            AND pmv.environment = '{env}'
            AND pmv.provision_result = 'success'
            AND pmv.user_email IS DISTINCT FROM '{EXCLUDED_EMAIL}'
          ORDER BY ps.provisioned_at DESC
          LIMIT {BASELINE_ROWS}
        )
        SELECT sel.uuid, sel.lifetime_hours, SUM(pc.total_cost) AS usage_amount
        FROM sel JOIN provision_cost pc ON pc.provision_uuid = sel.uuid
        WHERE pc.month_ts >= {lower} AND pc.month_ts <= {upper}
        GROUP BY sel.uuid, sel.lifetime_hours
    """)
    return baseline_from_rows(rows)


def confidence(n, days):
    if n < MIN_N or days > 7:
        return "low"
    return "high" if n >= 10 else "medium"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("uuid", help="provisions.uuid")
    ap.add_argument("--baseline", action="store_true",
                    help="also show the catalog-level baseline median as a cross-check")
    args = ap.parse_args()

    if not UUID_RE.match(args.uuid):
        sys.exit("error: not a valid provision UUID")
    token = os.environ.get("REPORTING_MCP_TOKEN")
    if not token:
        sys.exit("error: set REPORTING_MCP_TOKEN")
    mcp = MCP(os.environ.get("REPORTING_MCP_URL", DEFAULT_URL), token)

    prov = fetch_provision(mcp, args.uuid)
    if not prov:
        sys.exit(f"Provision {args.uuid} not found.")

    stats, used = None, None
    for days in WINDOWS_DAYS:
        stats, used = comparison_stats(mcp, prov, days), days
        if stats["n"] >= MIN_N:
            break

    print(f"Provision:  {prov['uuid']}")
    print(f"Catalog:    {prov['catalog_name']} (id {prov['catalog_id']})")
    print(f"Cloud/state: {prov['cloud']} / {prov['last_state']} (result: {prov['provision_result']}, "
          f"environment: {prov.get('environment') or 'unknown'})")
    print(f"Failing since {prov['provisioned_at']}: {prov['hours_failing']} h "
          "(provisioned_at is a proxy for failure start)")
    baseline = catalog_baseline(mcp, prov) if (args.baseline or not stats["n"]) else None
    hrs = float(prov["hours_failing"])
    if stats["n"]:
        src = f"Comparison: {stats['n']} provisions, {used} days before T0"
        conf = confidence(stats["n"], used)
        rates = stats
    elif baseline:
        src = (f"Comparison: none in {used} days before T0; using the catalog baseline "
               f"({baseline['n']} recent PROD provisions, outliers removed)")
        conf = "low"
        rates = baseline
    else:
        print(f"\nNo estimate possible: no successful comparison provisions in {used} days before T0, "
              "and no catalog baseline.")
        return
    med, lo, hi = (float(rates[k]) for k in ("median_per_hr", "min_per_hr", "max_per_hr"))
    print(src)
    print(f"Cost/hour:  median {med} (min {lo} - max {hi})")
    if args.baseline and stats["n"] and baseline:
        print(f"Cross-check: catalog baseline median {baseline['median_per_hr']} "
              f"(n={baseline['n']}, min {baseline['min_per_hr']} - max {baseline['max_per_hr']})")
    elif args.baseline and stats["n"]:
        print("Cross-check: no catalog baseline available.")
    print(f"\nESTIMATED COST: ${med * hrs:,.2f}  (range ${lo * hrs:,.2f} - ${hi * hrs:,.2f})")
    print(f"Confidence: {conf}")
    if med == hi:
        print("Note: median equals max; cost per hour is skewed.")
    print("This is an estimate, not billed cost; cost data can lag a day or more.")


if __name__ == "__main__":
    main()
