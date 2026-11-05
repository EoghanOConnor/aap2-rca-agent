# Provision cost estimate

Estimates what a failing provision is costing, given its provision UUID. This is
no longer a standalone skill: it is Step 4c of the
[root-cause-analysis skill](../root-cause-analysis/SKILL.md), and the logic lives
in `src/rca/analysis/cost_estimate.py`.

## Usage

```bash
export REPORTING_MCP_TOKEN=<token>
rca-analyze cost --job-id <job-id> [--provision-uuid <uuid>] [--baseline]
```

The UUID is read from the job name in `step1_job_context.json` (run
`rca-analyze analyze` first), or passed with `--provision-uuid`. The result is
printed and saved as `step4c_cost_estimate.json` in the job's analysis folder.

## Method

`median(cost per hour of similar successful provisions) × hours_failing`.
Similar means the same catalog item, retired in the 7 days before the failing
provision started; the window widens to 14 then 30 days until at least 3
provisions match. If none match, it falls back to a catalog-level median (always
low confidence; `--baseline` shows it as a cross-check). `provisioned_at` is used
as a proxy for failure start. The result is an estimate, not billed cost.

## Requirements

It queries the **`reporting-db-prod`** MCP server (read-only access to the RHDP
reporting PostgreSQL database) over HTTP itself, so it works without a connected
MCP client, but it needs `REPORTING_MCP_TOKEN` in the environment or in the
settings `env`. `env.example` lists the variables (`REPORTING_MCP_TOKEN`, and
optionally `REPORTING_MCP_URL` to override the default endpoint). Get the URL and
a token from the RHDP reporting platform team, and never commit the token.

Without a token the command exits with an error and produces no estimate.
