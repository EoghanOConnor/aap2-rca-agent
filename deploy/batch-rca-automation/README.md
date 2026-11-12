# Batch RCA Automation

The OpenShift CronJob runs the installed `rca-batch` Python entry point. The
orchestrator queries and filters jobs through a shared PostgreSQL connection
pool, runs deterministic analysis in bounded worker threads, invokes the
packaged RCA system prompt through the Claude Agent SDK, aggregates and stores
the batch report, and logs Agent SDK usage to MLflow when configured.

Each job uses one SDK query for investigation and its final structured summary.
The SDK validates the summary against the generated output schema and manages
structured-output retries. Python saves the successful structured result and
performs and verifies the analysis upload. SDK errors and missing structured
results fail the job.

The packaged system prompt is the source of per-job analysis instructions; no
on-disk Skill is copied into the workspace. Jira ticket preparation is
currently a no-op placeholder; the batch runner creates no Jira tickets.

`batch_rca_headless.sh` is retained as a compatibility wrapper for existing
manual/cron invocations and forwards all arguments to `rca-batch`.

## Configuration

`RCA_MAX_PARALLEL_JOBS` controls the maximum number of concurrent deterministic
job pipelines and Agent SDK queries. It must be a positive integer and defaults
to `5`. The Helm CronJob sets it from `cronjob.maxParallelJobs`.

## Development and tests

From the repository root:

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
```

PostgreSQL integration tests are under `tests/integration/` and can be run
with `tests/run_integration_tests.sh`.
