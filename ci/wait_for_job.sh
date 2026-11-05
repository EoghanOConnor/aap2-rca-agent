#!/usr/bin/env bash
# Block until the batch Job reaches a terminal state; used from the runner.
# Requires NS and JOB_NAME in the environment; optional WAIT_TIMEOUT_SECS
# (default 1200). Exits non-zero on failure or timeout, dumping the job logs.
set -euo pipefail

: "${NS:?NS must be set}" "${JOB_NAME:?JOB_NAME must be set}"
timeout_secs="${WAIT_TIMEOUT_SECS:-1200}"
deadline=$(( SECONDS + timeout_secs ))

while :; do
  # Use the Job's terminal conditions rather than pod counts: the Job retries
  # failed pods up to backoffLimit, so one failed pod is not a failed Job.
  failed=$(kubectl get job -n "$NS" "$JOB_NAME" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)
  complete=$(kubectl get job -n "$NS" "$JOB_NAME" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || true)

  if [[ "$failed" == "True" ]]; then
    echo "::error::batch job failed"
    kubectl logs -n "$NS" "job/$JOB_NAME" --all-containers || true
    exit 1
  fi
  if [[ "$complete" == "True" ]]; then
    echo "batch job completed"
    exit 0
  fi
  if (( SECONDS >= deadline )); then
    echo "::error::timed out after ${timeout_secs}s waiting for job/$JOB_NAME"
    kubectl logs -n "$NS" "job/$JOB_NAME" --all-containers || true
    exit 1
  fi
  sleep 10
done
