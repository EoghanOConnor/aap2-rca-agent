# aap2-rca-agent

Automated root-cause analysis for failed Ansible Automation Platform (AAP)
jobs on the Red Hat Demo Platform (RHDP). The Python package runs the batch
workflow and deterministic analysis; the existing on-disk Claude Code Skill
still provides per-job RCA guidance.

## Architecture

```mermaid
flowchart TD
    Start([OpenShift CronJob]) --> Runner[rca-batch]
    Runner --> Query[Query unprocessed jobs from PostgreSQL]
    Query --> AnyJobs{Jobs found?}
    AnyJobs -- No --> Done([Exit successfully])
    AnyJobs -- Yes --> Gate[Unified pre-analysis gate]
    Gate --> Known[Load recent high-confidence issues]
    Known --> Filter{Known-issue pre-filter enabled?}
    Filter -- Yes --> Prefilter[Cluster duplicates and match by shared scorer]
    Filter -- No --> Analysis
    Prefilter --> Matched[Store pre-matched jobs]
    Prefilter --> Analysis[Bounded parallel Python analysis]
    subgraph PerJob[Per-job analysis]
      Parse[Parse AAP job log]
      Splunk[Query Splunk]
      Correlation[Build correlation timeline]
      GitHub[Fetch AgnosticV config and AgnosticD code]
      Synthesis[Agent SDK invokes root-cause-analysis Skill]
      Upload[Upload analysis to Jumpbox]
      Parse --> Splunk --> Correlation --> GitHub --> Synthesis --> Upload
    end
    Analysis --> Parse
    Upload --> Aggregate[Semantic aggregation: historical matches and cross-job patterns]
    Known --> Aggregate
    Aggregate --> Write[Write report JSON]
    Write --> Store[Store one result per analyzed job and link canonical patterns]
    Store --> Jira[Step 6 placeholder: no Jira tickets created]
    Jira --> Done
```

## Installable Python package

The source tree uses the `src/rca` package layout. Runtime dependencies and
console entry points are declared in `pyproject.toml`; reusable modules are
installed once rather than copied into separate `common/` and batch-script
trees. `rca-batch` is the Python Agent SDK batch coordinator. The existing
on-disk Skill remains in place and is copied into the CronJob workspace.

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
```

`rca-analyze` preserves the manual single-job and debugging commands:

```bash
rca-analyze analyze --job-id 1234567 --fetch
rca-analyze analyze --job-log /path/to/job.json.gz
rca-analyze parse --job-log /path/to/job.json.gz
rca-analyze query 'index=ocp_apps "x1234"' --earliest=-24h
rca-analyze status 1234567
rca-analyze upload --job-id 1234567
```

The CronJob runs `rca-batch` directly. The legacy
`deploy/batch-rca-automation/batch_rca_headless.sh` path remains as a thin
compatibility wrapper that forwards arguments to `rca-batch`.

For a local batch run (after installing the package and configuring the
required settings):

```bash
rca-batch --limit 15
```

Use `--no-pre-filter` to skip matching jobs against recent known issues.
`RCA_MAX_PARALLEL_JOBS` sets the bound for parallel job pipelines and Agent SDK
queries (default `5`). The Step 6 Jira ticket preparation hook is intentionally
a no-op for now; no Jira integration or ticket creation is performed.

## Configuration and state

Configuration is read as data from `RCA_SETTINGS_FILE` (default discovery:
`./.claude/settings.json`, `./.claude/settings.local.json`, then
`~/.claude/settings.json`), `.env`, and the process environment. Precedence is
`.env` < `settings.json` < process environment. JSON values are parsed as
scalars and are not treated as shell code by Python.

Important settings include:

- `SOURCE_DB_*` for the AAP source and results tables.
- `JOB_LOGS_DIR`, `REMOTE_HOST`, and `REMOTE_DIR` for job-log retrieval.
- `SPLUNK_*` and `GITHUB_TOKEN` for deterministic enrichment.
- `JUMPBOX_URI` for uploading completed analysis when using a direct SSH target.
- Optional `SSH_JUMPBOX_ALIAS` to route uploads through a host configured in
  `~/.ssh/config` (the Helm cluster deployment uses `ci-jumpbox`). When set,
  SSH and rsync use the alias and take its host, user, and port from SSH config.
- `RCA_STATE_DIR` (default `~/.rca`) for writable reports and analysis state.
- `RCA_MAX_PARALLEL_JOBS` for bounded batch concurrency (default `5`).

Batch reports are written to `$RCA_STATE_DIR/reports/`. Per-job artifacts are
stored under `$RCA_STATE_DIR/.analysis/{job_id}/`. JSON schemas are included as
package resources and loaded with `importlib.resources`.

## Performance

| Metric | Value |
|---|---:|
| Jobs analyzed | 50+ jobs/day |
| Success rate | ~95% |
| Init time | ~14 seconds |
| Analysis time | 2–3 minutes for 5–7 jobs (parallel) |

These figures describe the existing production shell/Skill workflow; the
package-layout change is intended to preserve its behavior.

## Output

Batch reports are written to
`$RCA_STATE_DIR/reports/batch_YYYYMMDD_HHMMSS.json`:

```json
{
  "batch_id": "batch_YYYYMMDD_HHMMSS",
  "generated_at": "2026-09-30T12:00:00Z",
  "total_jobs_requested": 4,
  "total_jobs_analyzed": 3,
  "total_jobs_failed": 1,
  "timing": {
    "agent_spawn": "2026-09-30T12:00:00Z",
    "agent_completion": {},
    "wall_clock_total_ms": 150000,
    "aggregation_completed_at": "2026-09-30T12:02:30Z"
  },
  "root_cause_category_breakdown": {
    "infrastructure": {
      "count": 3,
      "job_ids": ["1234567", "1234568", "1234569"],
      "description": "Infrastructure"
    }
  },
  "confidence_breakdown": {"high": 2, "medium": 1, "low": 0},
  "high_priority_recommendations": [],
  "job_summaries": [
    {
      "job_id": "1234567",
      "result_id": 42,
      "status": "analyzed",
      "historical_matches": [
        {
          "matched_result_id": 9,
          "recurrence_count": 3,
          "similarity_reasoning": "Same worker timeout signature.",
          "pattern_id": "9",
          "confidence": "high",
          "evidence": [
            {
              "statement_type": "observed",
              "kind": "error_signature",
              "description": "Both jobs report worker 7 timing out.",
              "source": "splunk"
            }
          ]
        }
      ]
    }
  ],
  "failed_analyses": [],
  "cross_job_patterns": [
    {
      "pattern": "Worker 7 timeout",
      "jobs": ["1234567", "1234568"],
      "description": "Both jobs time out in worker 7.",
      "source": "current_batch",
      "pattern_id": "9",
      "confidence": "high",
      "evidence": [
        {
          "statement_type": "observed",
          "kind": "component",
          "description": "Worker 7 is the failing component in both jobs.",
          "source": "splunk"
        }
      ]
    }
  ]
}
```

`pattern_id` is a report signal representing the canonical result ID for a
linked issue cluster. The durable relationship is stored in
`related_result_id`, with `related_result_confidence` annotating the link. The
anchor row has a null `related_result_id`; every other cluster member points
directly to that anchor.
`historical_matches` and `cross_job_patterns` remain report signals, but the
durable grouping is the canonical `related_result_id` anchor. Every analyzed
job gets its own result row. A cluster can therefore be read with a single
equality query on `related_result_id` and does not require recursion.

Per-job analysis files are saved under `$RCA_STATE_DIR/.analysis/{job_id}/`:
job context, Splunk logs, the correlation timeline, GitHub fetch history, and
the Step 5 analysis summary.

## Deployment and tests

The Docker image uses `python:3.12-slim` and installs the package, including
the pinned Claude Agent SDK. The CronJob installs the on-disk Skill and
prepares SSH/GCP credentials in its init container before running `rca-batch`.

```bash
.venv/bin/pytest
tests/run_integration_tests.sh
helm lint deploy/helm -f deploy/helm/values.example.yaml
```

PostgreSQL integration tests use a separate local test database and skip when
it is unavailable. Never point the test database settings at production.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
