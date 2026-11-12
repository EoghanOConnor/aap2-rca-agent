#!/usr/bin/env bash
# One-time setup of a namespace for the batch CI workflow
# (batch-cronjob.yml).
#
# Creates only what the workflows don't create themselves:
#   1. claude-litellm-creds secret (skipped if it already exists)
#   2. long-lived token for the rca-ci-runner service account (the SA and
#      its rolebinding are expected to already exist — see --sa-namespace)
#   3. kubeconfig for the SA (default ~/.kube/rca-ci-runner.kubeconfig)
#   4. the rca-ci-jumpbox image (the only image push/PR runs never rebuild)
#   5. GitHub-side: KUBECONFIG_B64 secret + safe-to-test label (optional)
#
# The main rhdp-rca-plugin image is deliberately NOT built here: the
# workflows build it from the pushed commit on every push/PR run.
#
# Usage:
#   LITELLM_BASE_URL=... LITELLM_API_KEY=... ci/setup-namespace.sh my-ns
#   ci/setup-namespace.sh my-ns --litellm-url URL --litellm-key KEY
#   ci/setup-namespace.sh my-ns --skip-github      # cluster side only
#   ci/setup-namespace.sh my-ns --sa-namespace sa-ns
#
# Requires: oc + kubectl logged into the target cluster, with rights to
# create secrets in the SA's namespace; gh (authenticated) unless
# --skip-github. The litellm credentials are prompted for if not provided.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a; . ci/ci.env; set +a

usage() {
  sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
}

NS=""
KUBECONFIG_OUT="$HOME/.kube/rca-ci-runner.kubeconfig"
REPO=""
SKIP_GITHUB=0
LITELLM_URL="${LITELLM_BASE_URL:-}"
LITELLM_KEY="${LITELLM_API_KEY:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --litellm-url) LITELLM_URL="${2:?}"; shift 2 ;;
    --litellm-key) LITELLM_KEY="${2:?}"; shift 2 ;;
    --sa-namespace) SA_NS="${2:?}"; shift 2 ;;
    --kubeconfig) KUBECONFIG_OUT="${2:?}"; shift 2 ;;
    --repo) REPO="${2:?}"; shift 2 ;;
    --skip-github) SKIP_GITHUB=1; shift ;;
    -h|--help) usage ;;
    -*) echo "unknown flag: $1" >&2; usage ;;
    *) [[ -z "$NS" ]] || { echo "unexpected extra argument: $1" >&2; usage; }
       NS="$1"; shift ;;
  esac
done
[[ -n "$NS" ]] || usage
# The rca-ci-runner SA may live in a different namespace than the CI target
# (e.g. an SA in rh-ace-aiops with rights in both rh-ace-aiops and the test ns).
SA_NS="${SA_NS:-$NS}"

for tool in oc kubectl; do
  command -v "$tool" >/dev/null || { echo "error: $tool not found" >&2; exit 1; }
done

echo "==> Checking namespace $NS"
kubectl get namespace "$NS" >/dev/null \
  || { echo "error: namespace $NS does not exist — create it first (oc new-project $NS)" >&2; exit 1; }

# The SA and its rolebinding must already exist (a cluster admin creates them:
# oc create sa rca-ci-runner -n $SA_NS; oc adm policy add-role-to-user edit
# -z rca-ci-runner -n $NS). This script only mints the token secret for it.
echo "==> Service account rca-ci-runner in $SA_NS (token for CI)"
kubectl apply -n "$SA_NS" -f - <<'EOF'
apiVersion: v1
kind: Secret
type: kubernetes.io/service-account-token
metadata:
  name: rca-ci-runner-token
  annotations:
    kubernetes.io/service-account.name: rca-ci-runner
EOF

# The token controller populates the secret asynchronously.
TOKEN=""
for _ in $(seq 1 30); do
  TOKEN=$(kubectl get secret rca-ci-runner-token -n "$SA_NS" -o jsonpath='{.data.token}' 2>/dev/null | base64 -d)
  [[ -n "$TOKEN" ]] && break
  sleep 1
done
[[ -n "$TOKEN" ]] || { echo "error: rca-ci-runner-token not found or not populated in $SA_NS" >&2; exit 1; }

echo "==> Litellm secret $LITELLM_SECRET"
if kubectl get secret "$LITELLM_SECRET" -n "$NS" >/dev/null 2>&1; then
  echo "    already exists — leaving it untouched"
else
  if [[ -z "$LITELLM_URL" ]]; then read -rp "ANTHROPIC_BASE_URL: " LITELLM_URL; fi
  if [[ -z "$LITELLM_KEY" ]]; then read -rsp "ANTHROPIC_API_KEY: " LITELLM_KEY; echo; fi
  kubectl create secret generic "$LITELLM_SECRET" -n "$NS" \
    --from-literal=ANTHROPIC_BASE_URL="$LITELLM_URL" \
    --from-literal=ANTHROPIC_API_KEY="$LITELLM_KEY"
fi

echo "==> Writing $KUBECONFIG_OUT"
SERVER=$(oc whoami --show-server)
# CA is optional: empty works when the API cert is publicly signed (the
# client falls back to system roots); required for self-signed clusters.
CA=$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')
umask 077
{
  cat <<EOF
apiVersion: v1
kind: Config
clusters:
- name: ci-cluster
  cluster:
    server: $SERVER
EOF
  [[ -n "$CA" ]] && printf '    certificate-authority-data: %s\n' "$CA"
  cat <<EOF
users:
- name: rca-ci-runner
  user:
    token: $TOKEN
contexts:
- name: rca-ci/$NS/rca-ci-runner
  context:
    cluster: ci-cluster
    user: rca-ci-runner
    namespace: $NS
current-context: rca-ci/$NS/rca-ci-runner
EOF
} > "$KUBECONFIG_OUT"
chmod 600 "$KUBECONFIG_OUT"

echo "==> Verifying the SA kubeconfig"
kubectl --kubeconfig "$KUBECONFIG_OUT" -n "$NS" get secret "$LITELLM_SECRET" >/dev/null \
  || { echo "error: SA cannot read the litellm secret — rolebinding failed?" >&2; exit 1; }
kubectl --kubeconfig "$KUBECONFIG_OUT" -n "$NS" auth can-i create jobs | grep -q yes \
  || { echo "error: SA cannot create jobs — rolebinding failed?" >&2; exit 1; }
echo "    OK"

echo "==> Building the $JUMPBOX_NAME image"
kubectl get bc "$JUMPBOX_NAME" -n "$NS" >/dev/null 2>&1 \
  || oc new-build --binary --strategy docker --name "$JUMPBOX_NAME" -n "$NS"
oc start-build "$JUMPBOX_NAME" -n "$NS" --from-dir=ci/images/sshd --wait >/dev/null
echo "    built: image-registry.openshift-image-registry.svc:5000/$NS/$JUMPBOX_NAME:latest"

if [[ "$SKIP_GITHUB" -eq 1 ]]; then
  echo "==> Skipping GitHub setup (--skip-github)"
else
  command -v gh >/dev/null || { echo "error: gh not found (or pass --skip-github)" >&2; exit 1; }
  if [[ -z "$REPO" ]]; then
    REPO=$(git remote get-url origin | sed 's|.*github.com[:/]||; s|\.git$||')
  fi
  echo "==> GitHub secret KUBECONFIG_B64 on $REPO"
  base64 < "$KUBECONFIG_OUT" | tr -d '\n' | env -u GITHUB_TOKEN gh secret set KUBECONFIG_B64 -R "$REPO"
  echo "==> GitHub label safe-to-test"
  env -u GITHUB_TOKEN gh label create safe-to-test -R "$REPO" \
    --description "Run batch CI workflows on this PR (maintainer vouch)" --color 0E8A16 2>/dev/null \
    || echo "    already exists"
fi

echo
echo "Done. Namespace $NS is ready for the batch CI workflows."
echo "Notes:"
echo "  - The main rhdp-rca-plugin image is built by the workflows themselves"
echo "    on the first push/PR run."
echo "  - Trigger the first run: git commit --allow-empty -m 'ci: bootstrap' && git push"
