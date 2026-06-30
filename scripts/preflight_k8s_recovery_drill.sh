#!/bin/bash
# =============================================================================
# CedarGraph Kubernetes Recovery Drill
# =============================================================================
# Runs a disposable Kubernetes/Helm deployment in an isolated namespace, then
# exercises the same Raft identity evidence, recovery-plan, production-gate, and
# upgrade-guard checks used before production changes.
#
# This script is intentionally scoped to a drill namespace. It must not be used
# against a production namespace or as a production PVC cleanup mechanism.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
CHART_DIR="${CEDAR_DRILL_CHART_DIR:-${PROJECT_ROOT}/helm-chart/cedargraph}"
NAMESPACE="${CEDAR_DRILL_NAMESPACE:-cedargraph-recovery-drill}"
RELEASE="${CEDAR_DRILL_RELEASE:-recovery-drill}"
IMAGE_REPOSITORY="${CEDAR_DRILL_IMAGE_REPOSITORY:-cedargraph/cedar}"
IMAGE_TAG="${CEDAR_DRILL_IMAGE_TAG:-k8s-fix-20260630}"
EVIDENCE_ROOT="${CEDAR_DRILL_EVIDENCE_ROOT:-/tmp/cedargraph-recovery-drill-evidence}"
HELM_TIMEOUT="${CEDAR_DRILL_HELM_TIMEOUT:-10m}"
WAIT_TIMEOUT="${CEDAR_DRILL_WAIT_TIMEOUT:-600s}"
MIN_POD_AGE_SECONDS="${CEDAR_DRILL_MIN_POD_AGE_SECONDS:-300}"
CLEANUP="${CEDAR_DRILL_CLEANUP:-1}"
AUTH_SECRET="${CEDAR_DRILL_AUTH_SECRET:-${RELEASE}-cedargraph-graphd-auth}"
TLS_SECRET="${CEDAR_DRILL_TLS_SECRET:-${RELEASE}-cedargraph-graphd-tls}"

usage() {
  cat <<USAGE
Usage: $0

Environment:
  CEDAR_DRILL_NAMESPACE           Disposable namespace (default: cedargraph-recovery-drill)
  CEDAR_DRILL_RELEASE             Helm release name (default: recovery-drill)
  CEDAR_DRILL_IMAGE_REPOSITORY    Image repository (default: cedargraph/cedar)
  CEDAR_DRILL_IMAGE_TAG           Image tag (default: k8s-fix-20260630)
  CEDAR_DRILL_EVIDENCE_ROOT       Evidence output root (default: /tmp/cedargraph-recovery-drill-evidence)
  CEDAR_DRILL_MIN_POD_AGE_SECONDS Minimum pod age for production gate (default: 300)
  CEDAR_DRILL_CLEANUP             Delete drill namespace on success, 1 or 0 (default: 1)

The script creates and may delete only CEDAR_DRILL_NAMESPACE. It refuses to run
against common production namespace names.
USAGE
}

log_step() {
  echo "[STEP] $*"
}

die() {
  echo "$*" >&2
  exit 1
}

validate_bool() {
  local name="$1"
  local value="$2"
  [[ "${value}" == "0" || "${value}" == "1" ]] || die "${name} must be 0 or 1, got ${value}"
}

validate_non_negative_int() {
  local name="$1"
  local value="$2"
  [[ "${value}" =~ ^[0-9]+$ ]] || die "${name} must be a non-negative integer, got ${value}"
}

validate_positive_int() {
  local name="$1"
  local value="$2"
  [[ "${value}" =~ ^[0-9]+$ && "${value}" -ge 1 ]] ||
    die "${name} must be a positive integer, got ${value}"
}

validate_k8s_duration() {
  local name="$1"
  local value="$2"
  [[ "${value}" =~ ^[1-9][0-9]*(s|m|h)$ ]] ||
    die "${name} must match <positive-number><s|m|h>, got ${value}"
}

namespace_is_protected() {
  local namespace="$1"
  case "${namespace}" in
    default|kube-system|kube-public|kube-node-lease|cedargraph|cedargraph-preflight|prod|production|*-prod|prod-*|*-production|production-*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

run_self_test() {
  validate_bool CEDAR_DRILL_CLEANUP 0
  validate_bool CEDAR_DRILL_CLEANUP 1
  validate_non_negative_int CEDAR_DRILL_MIN_POD_AGE_SECONDS 0
  validate_non_negative_int CEDAR_DRILL_MIN_POD_AGE_SECONDS 300
  validate_positive_int CEDAR_DRILL_TLS_DAYS 365
  validate_k8s_duration CEDAR_DRILL_HELM_TIMEOUT 10m
  validate_k8s_duration CEDAR_DRILL_WAIT_TIMEOUT 600s
  if (validate_bool CEDAR_DRILL_CLEANUP true) >/dev/null 2>&1; then
    die "self-test failed: invalid cleanup bool should fail"
  fi
  if (validate_non_negative_int CEDAR_DRILL_MIN_POD_AGE_SECONDS -1) >/dev/null 2>&1; then
    die "self-test failed: negative min pod age should fail"
  fi
  if (validate_positive_int CEDAR_DRILL_TLS_DAYS 0) >/dev/null 2>&1; then
    die "self-test failed: zero TLS days should fail"
  fi
  if (validate_k8s_duration CEDAR_DRILL_HELM_TIMEOUT 0m) >/dev/null 2>&1; then
    die "self-test failed: zero Helm timeout should fail"
  fi
  if (validate_k8s_duration CEDAR_DRILL_WAIT_TIMEOUT 10d) >/dev/null 2>&1; then
    die "self-test failed: unsupported wait timeout unit should fail"
  fi

  for namespace in default kube-system cedargraph prod production cedargraph-prod prod-cedargraph cedargraph-production production-us; do
    namespace_is_protected "${namespace}" || die "self-test failed: namespace should be protected: ${namespace}"
  done
  namespace_is_protected cedargraph-recovery-drill &&
    die "self-test failed: default recovery drill namespace should be allowed"
  namespace_is_protected test-cedargraph &&
    die "self-test failed: non-production drill namespace should be allowed"

  echo "Kubernetes recovery drill self-test passed"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${1:-}" == "--self-test" ]]; then
  run_self_test
  exit 0
fi

validate_bool CEDAR_DRILL_CLEANUP "${CLEANUP}"
validate_non_negative_int CEDAR_DRILL_MIN_POD_AGE_SECONDS "${MIN_POD_AGE_SECONDS}"
validate_positive_int CEDAR_DRILL_TLS_DAYS "${CEDAR_DRILL_TLS_DAYS:-365}"
validate_k8s_duration CEDAR_DRILL_HELM_TIMEOUT "${HELM_TIMEOUT}"
validate_k8s_duration CEDAR_DRILL_WAIT_TIMEOUT "${WAIT_TIMEOUT}"
[[ -d "${CHART_DIR}" ]] || die "Chart directory does not exist: ${CHART_DIR}"

namespace_is_protected "${NAMESPACE}" &&
  die "Refusing to run recovery drill against protected namespace: ${NAMESPACE}"

for bin in kubectl helm openssl; do
  command -v "${bin}" >/dev/null 2>&1 || die "${bin} is required"
done

evidence_dir="${EVIDENCE_ROOT}/$(date -u '+%Y%m%dT%H%M%SZ')"
mkdir -p "${evidence_dir}"

cleanup() {
  local exit_code=$?
  if [[ "${exit_code}" == "0" && "${CLEANUP}" == "1" ]]; then
    log_step "Cleanup drill namespace"
    if ! helm uninstall "${RELEASE}" -n "${NAMESPACE}" >/dev/null 2>"${evidence_dir}/cleanup-helm-uninstall.err"; then
      echo "[ERROR] Failed to uninstall drill Helm release ${RELEASE}; see ${evidence_dir}/cleanup-helm-uninstall.err" >&2
      echo "[INFO] Set CEDAR_DRILL_CLEANUP=0 before rerunning if you want to keep the drill namespace for inspection." >&2
      exit 1
    fi
    if ! kubectl delete namespace "${NAMESPACE}" --wait=true >/dev/null 2>"${evidence_dir}/cleanup-namespace-delete.err"; then
      echo "[ERROR] Failed to delete drill namespace ${NAMESPACE}; see ${evidence_dir}/cleanup-namespace-delete.err" >&2
      echo "[INFO] Set CEDAR_DRILL_CLEANUP=0 before rerunning if you want to keep the drill namespace for inspection." >&2
      exit 1
    fi
  else
    echo "[INFO] Drill namespace kept for inspection: ${NAMESPACE}"
  fi
  exit "${exit_code}"
}
trap cleanup EXIT

log_step "Create isolated namespace"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

log_step "Create GraphD auth Secret"
jwt_secret="$(openssl rand -base64 48)"
password="$(openssl rand -base64 24)"
kubectl create secret generic "${AUTH_SECRET}" -n "${NAMESPACE}" \
  --from-literal=jwt-secret="${jwt_secret}" \
  --from-literal=user="cedar-admin" \
  --from-literal=password="${password}" \
  --from-literal=role="admin" \
  --dry-run=client -o yaml | kubectl apply -f -

log_step "Create TLS Secret with service and StatefulSet Pod SANs"
CEDAR_K8S_NAMESPACE="${NAMESPACE}" \
CEDAR_HELM_RELEASE="${RELEASE}" \
CEDAR_TLS_SECRET_NAME="${TLS_SECRET}" \
CEDAR_EXPECTED_METAD_PODS=3 \
CEDAR_EXPECTED_STORAGED_PODS=3 \
CEDAR_TLS_DAYS="${CEDAR_DRILL_TLS_DAYS:-365}" \
  "${SCRIPT_DIR}/generate_k8s_tls_secret.sh" --apply

log_step "Install Helm release"
helm upgrade --install "${RELEASE}" "${CHART_DIR}" -n "${NAMESPACE}" \
  --set image.repository="${IMAGE_REPOSITORY}" \
  --set image.tag="${IMAGE_TAG}" \
  --set image.pullPolicy=IfNotPresent \
  --set global.storageClass="${CEDAR_DRILL_STORAGE_CLASS:-standard}" \
  --set metad.resources.requests.memory="${CEDAR_DRILL_METAD_REQUEST_MEMORY:-128Mi}" \
  --set metad.resources.requests.cpu="${CEDAR_DRILL_METAD_REQUEST_CPU:-100m}" \
  --set metad.resources.limits.memory="${CEDAR_DRILL_METAD_LIMIT_MEMORY:-512Mi}" \
  --set metad.resources.limits.cpu="${CEDAR_DRILL_METAD_LIMIT_CPU:-500m}" \
  --set metad.persistence.size="${CEDAR_DRILL_METAD_STORAGE_SIZE:-512Mi}" \
  --set storaged.resources.requests.memory="${CEDAR_DRILL_STORAGED_REQUEST_MEMORY:-256Mi}" \
  --set storaged.resources.requests.cpu="${CEDAR_DRILL_STORAGED_REQUEST_CPU:-100m}" \
  --set storaged.resources.limits.memory="${CEDAR_DRILL_STORAGED_LIMIT_MEMORY:-768Mi}" \
  --set storaged.resources.limits.cpu="${CEDAR_DRILL_STORAGED_LIMIT_CPU:-500m}" \
  --set storaged.persistence.size="${CEDAR_DRILL_STORAGED_STORAGE_SIZE:-1Gi}" \
  --set graphd.resources.requests.memory="${CEDAR_DRILL_GRAPHD_REQUEST_MEMORY:-128Mi}" \
  --set graphd.resources.requests.cpu="${CEDAR_DRILL_GRAPHD_REQUEST_CPU:-100m}" \
  --set graphd.resources.limits.memory="${CEDAR_DRILL_GRAPHD_LIMIT_MEMORY:-512Mi}" \
  --set graphd.resources.limits.cpu="${CEDAR_DRILL_GRAPHD_LIMIT_CPU:-500m}" \
  --set graphd.auth.existingSecret="${AUTH_SECRET}" \
  --set graphd.tls.existingSecret="${TLS_SECRET}" \
  --set graphd.tls.enabled=true \
  --set networkPolicy.enabled=true \
  --set networkPolicy.ingress[0].from[0].namespaceSelector.matchLabels.kubernetes\\.io/metadata\\.name="${NAMESPACE}" \
  --set networkPolicy.ingress[0].ports[0].protocol=TCP \
  --set networkPolicy.ingress[0].ports[0].port=7000 \
  --set networkPolicy.ingress[0].ports[1].protocol=TCP \
  --set networkPolicy.ingress[0].ports[1].port=7001 \
  --set networkPolicy.ingress[0].ports[2].protocol=TCP \
  --set networkPolicy.ingress[0].ports[2].port=9559 \
  --set networkPolicy.ingress[0].ports[3].protocol=TCP \
  --set networkPolicy.ingress[0].ports[3].port=10559 \
  --set networkPolicy.ingress[0].ports[4].protocol=TCP \
  --set networkPolicy.ingress[0].ports[4].port=9667 \
  --set networkPolicy.ingress[0].ports[5].protocol=TCP \
  --set networkPolicy.ingress[0].ports[5].port=9668 \
  --set networkPolicy.ingress[0].ports[6].protocol=TCP \
  --set networkPolicy.ingress[0].ports[6].port=9669 \
  --set networkPolicy.ingress[0].ports[7].protocol=TCP \
  --set networkPolicy.ingress[0].ports[7].port=9780 \
  --set networkPolicy.ingress[0].ports[8].protocol=TCP \
  --set networkPolicy.ingress[0].ports[8].port=9779 \
  --set metad.updateStrategy.type=OnDelete \
  --set metad.allowUnsafeRollingUpdate=false \
  --wait --timeout "${HELM_TIMEOUT}"

log_step "Wait for workload rollout"
kubectl wait pod -n "${NAMESPACE}" \
  -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=metad" \
  --for=condition=Ready --timeout="${WAIT_TIMEOUT}"
kubectl wait pod -n "${NAMESPACE}" \
  -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=storaged" \
  --for=condition=Ready --timeout="${WAIT_TIMEOUT}"
kubectl rollout status deployment/"${RELEASE}-cedargraph-graphd" -n "${NAMESPACE}" --timeout="${WAIT_TIMEOUT}"

log_step "Collect and verify MetaD Raft evidence"
raft_evidence="${evidence_dir}/metad-raft"
CEDAR_K8S_NAMESPACE="${NAMESPACE}" CEDAR_HELM_RELEASE="${RELEASE}" \
  "${SCRIPT_DIR}/preflight_k8s_raft_identity.sh" --collect-evidence "${raft_evidence}"
"${SCRIPT_DIR}/preflight_k8s_raft_identity.sh" --verify-evidence "${raft_evidence}"
"${SCRIPT_DIR}/preflight_k8s_raft_identity.sh" --plan-recovery "${raft_evidence}"

log_step "Run production gate against drill release"
CEDAR_K8S_NAMESPACE="${NAMESPACE}" \
CEDAR_HELM_RELEASE="${RELEASE}" \
CEDAR_MIN_POD_AGE_SECONDS="${MIN_POD_AGE_SECONDS}" \
  "${SCRIPT_DIR}/preflight_k8s_production_gate.sh"

log_step "Verify upgrade guard blocks normal MetaD rolling upgrade"
set +e
CEDAR_K8S_NAMESPACE="${NAMESPACE}" \
CEDAR_HELM_RELEASE="${RELEASE}" \
CEDAR_MIN_POD_AGE_SECONDS="${MIN_POD_AGE_SECONDS}" \
CEDAR_RUN_RAFT_UPGRADE_GUARD=1 \
  "${SCRIPT_DIR}/preflight_k8s_production_gate.sh" > "${evidence_dir}/upgrade-guard.log" 2>&1
guard_rc=$?
set -e
if [[ "${guard_rc}" == "0" ]]; then
  cat "${evidence_dir}/upgrade-guard.log" >&2
  die "Upgrade guard unexpectedly passed; drill must prove normal MetaD rolling upgrade is blocked"
fi
grep -q "MetaD Raft upgrade guard failed" "${evidence_dir}/upgrade-guard.log" ||
  die "Upgrade guard failed for an unexpected reason; see ${evidence_dir}/upgrade-guard.log"

cat > "${evidence_dir}/SUMMARY.txt" <<EOF
CedarGraph Kubernetes recovery drill passed.

Namespace: ${NAMESPACE}
Release: ${RELEASE}
Image: ${IMAGE_REPOSITORY}:${IMAGE_TAG}
Evidence: ${evidence_dir}

Verified:
  - disposable Helm install reached Ready state
  - MetaD Raft evidence bundle collected and verified
  - recovery plan generated from evidence
  - production gate passed for the drill release
  - upgrade guard blocked normal MetaD rolling upgrade

Scope:
  - This drill proves the gate/evidence/recovery-plan chain is repeatable.
  - It does not mutate production namespaces or delete production PVCs.
EOF

echo "Kubernetes recovery drill passed; evidence written to ${evidence_dir}"
