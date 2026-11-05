# CI testing flow

Two workflows run in CI:

- **`tests.yml`**: unit and integration tests. Runs on every push and every PR.
- **`batch-cronjob.yml`**: installs the real Helm chart on OpenShift and runs one batch from the actual CronJob. Everything it talks to is synthetic: a CI Postgres, a CI SSH jumpbox and generated job logs.

All CI names, ports, DB settings and the model live in `ci/ci.env`. The workflow loads it into the job environment and renders the manifests, Helm flags and settings JSON from it; `ci/setup-namespace.sh` sources it too.

## Overview

```mermaid
flowchart LR
    push([push to any branch]) --> tests[tests.yml]
    pr([pull_request]) --> tests
    tests --> unit[Unit tests<br/>Python 3.11 + 3.13]
    tests --> integ[Integration tests<br/>Postgres]

    pushOcp([push to ocp-testing]) --> cron[batch-cronjob.yml]
    label([PR labeled<br/>safe-to-test]) --> cron
    dispatch([workflow_dispatch]) --> cron
    cron --> cluster[(OpenShift namespace)]
```

## `tests.yml`

```mermaid
flowchart TD
    start([push / pull_request]) --> conc{"Concurrency group<br/>workflow + ref<br/>(cancel in-progress)"}
    conc --> U & I

    subgraph U["unit job (matrix: 3.11, 3.13)"]
        u1[checkout] --> u2[setup-python + pip cache]
        u2 --> u3["pip install -e .[dev]"]
        u3 --> u4["pytest -m 'not integration'"]
    end

    subgraph I["integration job (3.13)"]
        i1[checkout] --> i2[setup-python + pip cache]
        i2 --> i3["pip install -e .[dev]"]
        i3 --> i4[tests/run_integration_tests.sh]
    end
```

## `batch-cronjob.yml`

```mermaid
flowchart TD
    T([push to ocp-testing<br/>PR labeled safe-to-test<br/>workflow_dispatch]) --> G{"PR event without<br/>the safe-to-test label?"}
    G -- yes --> SKIP([skipped])
    G -- no --> SETUP["Connect to the cluster<br/>kubeconfig secret, namespace, oc"]
    SETUP --> BUILD["Build images in the cluster<br/>rca app (this commit) + CI jumpbox"]
    BUILD --> INFRA["Deploy synthetic infrastructure<br/>CI Postgres + CI SSH jumpbox with a fresh keypair"]
    INFRA --> HELM["helm install the real chart<br/>values from ci/ci.env, LiteLLM creds from the cluster"]
    HELM --> SEED["Seed Postgres and write<br/>synthetic job logs to the chart PVC"]
    SEED --> RUN["Run one Job from the CronJob<br/>wait until it completes, fails or times out"]
    RUN --> VERIFY["Verify the batch report<br/>and the result uploads on the jumpbox"]
    VERIFY --> ART["Always: upload report and logs as an artifact"]
    ART --> CLEAN["Delete CI resources<br/>unless CI_SKIP_CLEANUP=true"]
```

- **Trust gate:** `pull_request_target` runs the PR's code with cluster credentials, so
  PR runs only start when a maintainer adds the `safe-to-test` label.
- **One run at a time:** runs share a concurrency group and queue rather than cancel.
- **Fail fast:** if any step fails, the run skips straight to artifact upload and cleanup.
- **What is checked:** `ci/verify_report.py` checks the report's structure and counts, not the LLM text.
  Every job marked `analyzed` must also have an upload directory on the jumpbox.

### What the batch Job does in the cluster

The chart's SSH settings point at the CI jumpbox and its database settings at the CI Postgres,
so the CronJob uses its normal tunnel and upload paths against synthetic infrastructure only.

```mermaid
flowchart LR
    JOB["Batch Job<br/>(from the chart's CronJob)"] -- "SSH tunnel" --> JB["CI jumpbox"]
    JB --> PG[("CI Postgres")]
    JOB -- "upload results over SSH" --> JB
    JOB -- "read logs, write report" --> PVC[("chart PVC")]
    JOB -- "LLM calls" --> LLM["LiteLLM"]
    JOB -. "not configured" .-> SKIPPED["Splunk / GitHub / Jira<br/>steps skipped"]
```
