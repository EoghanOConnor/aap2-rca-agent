---
name: provision-cost-estimate
description: Estimate the cost of a failing provision given its UUID. Looks up the provision in the RHDP reporting database, finds successful provisions of the same catalog item from the 7 days before the failure, derives a cost per hour, and multiplies it by the hours the provision has been failing. Use when asked what a failed or failing provision is costing, or to add a cost estimate to an RCA.
license: MIT
allowed-tools:
  - mcp__reporting-db-prod__query
  - mcp__reporting-db-prod__describe_table
---

# Provision Cost Estimate

Estimate what a failing provision is costing, using the cost of similar provisions from the 7 days before it failed.

**This is an estimate, not billed cost.** Always label it as such in the output.

## Input

A provision UUID (`provisions.uuid`). If none is supplied, ask for one.

## Method

```
estimated_cost = median(cost per hour of similar provisions) × hours_failing
```

All queries use the `mcp__reporting-db-prod__query` tool. `provisions` and `provision_cost` are partitioned by `month_ts`, so every query must filter on it.

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

**Anchor the window to `<T0>`, not to `now()`.** The comparison set is the 7 days before the failing provision started, so the estimate is the same whether the failure is live or months old. The `month_ts` lower bound is `<T0> - 37 days`, wider than the window on purpose: `month_ts` is the month a provision *started*, so a provision retired inside the window may have started up to a month earlier.

Similar means: same `catalog_id`, `provision_result = 'success'`, and retired inside the window. Retired provisions are used so their cost is complete.

```sql
WITH cmp AS (
  SELECT p.uuid,
         GREATEST(EXTRACT(epoch FROM p.retired_at - p.provisioned_at) / 3600, 1) AS hrs
  FROM provisions p
  WHERE p.month_ts >= DATE_TRUNC('month', TIMESTAMP '<T0>' - interval '37 days')
    AND p.month_ts <= DATE_TRUNC('month', TIMESTAMP '<T0>')
    AND p.catalog_id = <CATALOG_ID>
    AND p.uuid <> '<UUID>'
    AND p.provision_result = 'success'
    AND p.retired_at >= TIMESTAMP '<T0>' - interval '7 days'
    AND p.retired_at <  TIMESTAMP '<T0>'
), cost AS (
  SELECT pc.provision_uuid, SUM(pc.total_cost) AS total_cost
  FROM provision_cost pc
  WHERE pc.month_ts >= DATE_TRUNC('month', TIMESTAMP '<T0>' - interval '37 days')
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

If `n < 3`, repeat with the window widened to 14 days, then 30 days, and report the window that was used. If `n` is still 0, report that no estimate is possible rather than guessing.

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

- This is an estimate. It is not billed cost, and cost data can lag by a day or more.
- Failures with `retired_at` set stop accruing cost at retirement. The hours calculation already stops there.
- If the median equals the max, the cost per hour is skewed. Say so.
