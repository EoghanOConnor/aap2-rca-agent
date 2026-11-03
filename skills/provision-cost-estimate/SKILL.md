---
name: provision-cost-estimate
description: Estimate the cost of a failing provision given its UUID. Looks up the provision in the RHDP reporting database, finds successful provisions of the same catalog item from the 7 days before the failure, derives a cost per hour, and multiplies it by the hours the provision has been failing. Use when asked what a failed or failing provision is costing, or to add a cost estimate to an RCA.
license: MIT
allowed-tools:
  - mcp__reporting-db-prod__query
  - mcp__reporting-db-prod__describe_table
  - Bash(python3 -I *catalog_metrics.py *)
---

# Provision Cost Estimate

Estimate what a failing provision is costing, using the cost of similar provisions from the 7 days before it failed.

**This is an estimate, not billed cost.** Always label it as such in the output.

## Input

A provision UUID (`provisions.uuid`). If none is supplied, ask for one.

## Preferred: run the script

`scripts/catalog_metrics.py` (next to this file) implements the method below end to end, including the 7 → 14 → 30 day widening and the confidence rating. Run it first:

```bash
python3 -I <skill-dir>/scripts/catalog_metrics.py <provision-uuid>
```

It needs `REPORTING_MCP_TOKEN` in the environment (and optionally `REPORTING_MCP_URL`; it defaults to the production reporting MCP endpoint). `env.example` in this folder lists both.

The estimate always uses the **median** cost per hour. If the 7 → 14 → 30 day comparison finds no provisions, the script falls back to a **catalog baseline**: the median cost per lifetime hour of the catalog item's most recent successful provisions in the same environment (up to 400 within 180 days, infrastructure user excluded, outliers removed with a 1.5 × IQR filter). This is the method demo-reporting's catalog metrics use, minus its flaws: provisions with no cost rows are skipped instead of counted as zero. The baseline is always reported as low confidence. Pass `--baseline` to print it as a cross-check beside the normal estimate. It talks to the same `reporting-db-prod` MCP server over HTTP, so it does not use this session's MCP connection. Never print or log the token.

Relay its output as the result: it already states the estimate, range, window, `n`, confidence and caveats. Add the **Caveats to state** below if the output omits any.

If `REPORTING_MCP_TOKEN` is not set, or the script exits with an error, say so and fall back to the manual method below. Do not guess a token. A script error is not "no estimate possible"; only the script's own "No estimate possible" message, or the manual method finding `n = 0` at 30 days, means that.

## Method

The script and the manual steps compute the same thing. Keep them in step: if you change one, change the other.

```
estimated_cost = median(cost per hour of similar provisions) × hours_failing
```

For the manual method, all queries use the `mcp__reporting-db-prod__query` tool. `provisions` and `provision_cost` are partitioned by `month_ts`, so every query must filter on it.

### Step 1: Fetch the provision

```sql
SELECT p.uuid, p.catalog_id, p.provision_result, p.last_state, p.healthy,
       p.cloud, p.provisioned_at, p.retired_at, ci.display_name AS catalog_name
FROM provisions p
LEFT JOIN catalog_items ci ON ci.id = p.catalog_id
WHERE p.uuid = '<UUID>'
  AND p.month_ts >= DATE_TRUNC('month', now() - interval '6 months')
```

If no row comes back, widen the `month_ts` lower bound once. If it is still missing, stop and report that the UUID was not found.

Record `catalog_id`, `provisioned_at` (call it `<T0>`), `retired_at`, `last_state`.

### Step 2: Hours failing

```
hours_failing = (COALESCE(retired_at, now()) - provisioned_at) in hours
```

`provisioned_at` is the closest available proxy for when the failure started, because the table has no failure timestamp. State that assumption in the output.

### Step 3: Cost per hour of similar provisions (7-day window)

**Anchor the window to `<T0>`, not to `now()`.** The comparison set is the 7 days before the failing provision started, so the estimate is the same whether the failure is live or months old. The `month_ts` lower bound is `<T0> - (<WINDOW_DAYS> + 31) days` (37 days for the 7-day window, 45 for 14, 61 for 30), wider than the window on purpose: `month_ts` is the month a provision *started*, so a provision retired inside the window may have started up to a month earlier.

Similar means: same `catalog_id`, `provision_result = 'success'`, and retired inside the window. Retired provisions are used so their cost is complete.

```sql
WITH cmp AS (
  SELECT p.uuid,
         GREATEST(EXTRACT(epoch FROM p.retired_at - p.provisioned_at) / 3600, 1) AS hrs
  FROM provisions p
  WHERE p.month_ts >= DATE_TRUNC('month', TIMESTAMP '<T0>' - interval '<WINDOW_DAYS + 31> days')
    AND p.month_ts <= DATE_TRUNC('month', TIMESTAMP '<T0>')
    AND p.catalog_id = <CATALOG_ID>
    AND p.uuid <> '<UUID>'
    AND p.provision_result = 'success'
    AND p.retired_at >= TIMESTAMP '<T0>' - interval '<WINDOW_DAYS> days'
    AND p.retired_at <  TIMESTAMP '<T0>'
), cost AS (
  SELECT pc.provision_uuid, SUM(pc.total_cost) AS total_cost
  FROM provision_cost pc
  WHERE pc.month_ts >= DATE_TRUNC('month', TIMESTAMP '<T0>' - interval '<WINDOW_DAYS + 31> days')
    AND pc.month_ts <= DATE_TRUNC('month', TIMESTAMP '<T0>')
    AND pc.provision_uuid IN (SELECT uuid FROM cmp)
  GROUP BY pc.provision_uuid
)
SELECT COUNT(*) AS n,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY c.total_cost / cmp.hrs)::numeric, 4) AS median_per_hr,
       ROUND(MIN(c.total_cost / cmp.hrs)::numeric, 4) AS min_per_hr,
       ROUND(MAX(c.total_cost / cmp.hrs)::numeric, 4) AS max_per_hr
FROM cmp JOIN cost c ON c.provision_uuid = cmp.uuid
```

If `n < 3`, repeat with the window widened to 14 days, then 30 days, and report the window that was used. If `n` is still 0, report that no estimate is possible rather than guessing. (The script instead falls back to the catalog baseline described above; the manual method has no equivalent.)

### Step 4: Calculate

```
low  = min_per_hr    × hours_failing
mid  = median_per_hr × hours_failing
high = max_per_hr    × hours_failing
```

## Output

Report:

- Provision: uuid, catalog item, cloud, last state
- Failing since `provisioned_at` (**hours_failing**, with the proxy caveat)
- Comparison set: window used (anchored at `<T0>`), `n`
- Cost per hour: median (min–max)
- **Estimated cost: `$mid` (range `$low`–`$high`)**
- Confidence: high (`n >= 10`, 7-day window), medium (`n` 3–9), low (`n < 3` or a widened window)

## Caveats to state

- `n` counts only comparison provisions that have `provision_cost` rows. Retired successful provisions with no cost rows yet are dropped (the join is inner), so `n` can be well below the number of matching provisions and can trigger the widening.
- This is an estimate. It is not billed cost, and cost data can lag by a day or more.
- Failures with `retired_at` set stop accruing cost at retirement. The hours calculation already stops there.
- If the median equals the max, the cost per hour is skewed. Say so.
