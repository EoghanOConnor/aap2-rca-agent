**Summary:** AWS EC2 RunInstances failed: RequestLimitExceeded during instance launch

**Issue type:** Bug
**Priority:** High
**Labels:** `aap-rca`, `cloud_api`, `auto-filed`

| Field | Value |
|---|---|
| Job ID | 21 |
| Catalog item | aws-blank-open-environment |
| Platform | aws |
| Batch | batch_20261001_143000 |
| Root cause category | cloud_api |
| Confidence | high |
| Job duration | 298s |
| Failing role | aws_instance_create |
| Analysis artifacts | `/workspace/.analysis/21/` |

## Root cause

AWS EC2 RunInstances failed: RequestLimitExceeded during instance launch.

## Causal chain

1. **AWS throttled RunInstances at the account level** during a burst of
   concurrent provisioning jobs. *(evidence 1)*
2. *Contributing:* **the instance-creation role sets no retry backoff**, so
   boto3 defaults to 3 attempts with no jitter. *(evidence 3)*
3. **Retries exhausted before the rate limit cleared**, so the task failed
   and the job aborted. *(evidence 2)*
4. *Contributing:* **the same signature hit unrelated catalog items in the
   same window**, confirming an account-level limit rather than a
   per-catalog config fault. *(evidence 4)*

## Evidence

| # | Source | Timestamp | Detail |
|---|---|---|---|
| 1 | aap_job | 14:06:02Z | Task 'Launch EC2 instance' failed: An error occurred (RequestLimitExceeded) when calling the RunInstances operation |
| 2 | splunk_ocp | 14:06:03Z | `ansible-runner-x7f33` stderr: boto3 retry exhausted after 3 attempts |
| 3 | agnosticd_code | — | `amazon.aws.ec2_instance` called with no retries/backoff config; inherits boto3 standard mode (3 attempts, no jitter) — `rhpds/agnosticd:ansible/roles_ocp_workloads/aws_instance_create/tasks/main.yml:34` |
| 4 | related_job | 14:02:55Z | Job 20 failed with the identical RequestLimitExceeded signature 3 minutes earlier on a different catalog_item (openshift-cnv-single-node) |

## Ruled out

- **DNS resolution failure in the provisioning namespace.** Looked
  plausible because the task aborted at the connection stage, which
  usually correlates with DNS problems in this environment. Ruled out:
  Splunk shows successful DNS resolution 2s before the failure; the error
  was an AWS API throttle, not a lookup failure. *(evidence 2)*

## Recommendations

**1. [High] Add exponential backoff with jitter to the EC2 instance-creation role**
`rhpds/agnosticd:ansible/roles_ocp_workloads/aws_instance_create/tasks/main.yml:34`
`retries: {mode: adaptive, max_attempts: 8}`

> Proposed patch — model-authored, not tested. Base: `a3f91c2`.

```diff
--- a/ansible/roles_ocp_workloads/aws_instance_create/tasks/main.yml
+++ b/ansible/roles_ocp_workloads/aws_instance_create/tasks/main.yml
@@ -31,6 +31,9 @@
     instance_type: "{{ aws_instance_type }}"
     image_id: "{{ aws_ami_id }}"
     wait: true
+    retries:
+      mode: adaptive
+      max_attempts: 8
```

**2. [Medium] Cap concurrent RunInstances calls per region in the batch scheduler**
Spreads provisioning load so the account-level limit is not hit in bursts.
No patch — not a code change in this repo.

## Possibly related (same batch, pattern-matched, unconfirmed)

Flagged by the analysis as sharing a failure signature with this issue, but
**not** verified by deterministic matching. Review before assuming the same
root cause.

Pattern: `aws_ec2_request_limit_exceeded` — same RequestLimitExceeded
signature recurring across unrelated catalog items in the same batch
window.

- Job 20 (row 501) — openshift-cnv-single-node
- Job 22 (row 503) — rhel9-vm-standard
- Job 24 (row 504) — ocp-shared-cluster-ns
