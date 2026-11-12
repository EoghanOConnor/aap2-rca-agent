#!/bin/bash
set -euo pipefail

POD_NAME="${POD_NAME:-rhdp-rca-plugin-eval}"

usage() {
  cat <<EOF
Usage: $0 <job-id>

Run the installed single-job RCA entry point inside the OpenShift eval pod.
The eval pod uses the same image, settings, and SSH configuration as the CronJob.

Environment variables:
  POD_NAME       Eval pod name (default: rhdp-rca-plugin-eval)
  JOB_LOGS_DIR   Optional in-pod override used to exercise preflight behavior
EOF
  exit 1
}

if [[ $# -ne 1 ]]; then
  usage
fi

JOB_ID="$1"
if [[ ! "$JOB_ID" =~ ^[0-9]+$ ]]; then
  echo "job-id must be numeric" >&2
  exit 1
fi

echo "==> Checking pod status..."
oc wait "pod/${POD_NAME}" --for=condition=Ready --timeout=60s

echo "==> Running rca-analyze for job ${JOB_ID}..."
REMOTE_COMMAND=(rca-analyze analyze --job-id "${JOB_ID}" --fetch)
REMOTE_ENV=(env)
if [[ -n "${JOB_LOGS_DIR:-}" ]]; then
  REMOTE_ENV+=("JOB_LOGS_DIR=${JOB_LOGS_DIR}")
fi

oc exec "${POD_NAME}" -- "${REMOTE_ENV[@]}" bash -c '
  DB_REMOTE_HOST=$(cat /tmp/ssh-secret/db-remote-host)
  DB_REMOTE_PORT=$(cat /tmp/ssh-secret/db-remote-port)
  ssh -f -N -L "15432:${DB_REMOTE_HOST}:${DB_REMOTE_PORT}" aap2-bastion
  exec "$@"
' bash "${REMOTE_COMMAND[@]}"
