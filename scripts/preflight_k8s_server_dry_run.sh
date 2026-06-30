#!/bin/bash
# =============================================================================
# CedarGraph Kubernetes API Server Dry-Run Preflight
# =============================================================================
# Renders the Kustomize and Helm manifests, then asks a real Kubernetes API
# server to validate them with server-side dry-run. No resources are created.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
NAMESPACE="${CEDAR_K8S_NAMESPACE:-cedargraph}"
KUSTOMIZE_RENDERED="${CEDAR_K8S_DRY_RUN_KUSTOMIZE_RENDERED:-/tmp/cedargraph-kustomize-server-dry-run.yaml}"
HELM_RENDERED="${CEDAR_K8S_DRY_RUN_HELM_RENDERED:-/tmp/cedargraph-helm-server-dry-run.yaml}"

cd "${PROJECT_ROOT}"

require_command() {
    local cmd="$1"
    command -v "${cmd}" >/dev/null 2>&1 || {
        echo "Required command not found: ${cmd}" >&2
        exit 1
    }
}

run_dry_run() {
    local name="$1"
    shift
    local log_file
    log_file="$(mktemp "${TMPDIR:-/tmp}/cedar-${name}-server-dry-run.XXXXXX")"

    if ! "$@" >"${log_file}" 2>&1; then
        cat "${log_file}" >&2
        echo "${name} server-side dry-run failed" >&2
        rm -f "${log_file}"
        exit 1
    fi

    if grep -E "Warning:|Error:" "${log_file}" | grep -v "Warning: spec.SessionAffinity is ignored for headless services" >&2; then
        echo "${name} server-side dry-run produced unexpected warnings/errors" >&2
        rm -f "${log_file}"
        exit 1
    fi

    cat "${log_file}"
    rm -f "${log_file}"
}

require_command kubectl
require_command helm

kubectl cluster-info >/dev/null

kubectl kustomize k8s > "${KUSTOMIZE_RENDERED}"
helm template cedargraph helm-chart/cedargraph \
    --namespace "${NAMESPACE}" \
    --set graphd.auth.existingSecret=graphd-auth \
    --set graphd.tls.existingSecret=graphd-tls \
    > "${HELM_RENDERED}"

run_dry_run kustomize kubectl apply --dry-run=server -f "${KUSTOMIZE_RENDERED}"
run_dry_run helm kubectl apply --dry-run=server -n "${NAMESPACE}" -f "${HELM_RENDERED}"

echo "Kubernetes API server dry-run preflight passed"
