#!/bin/bash
# =============================================================================
# CedarGraph Release Preflight Gate
# =============================================================================
# Runs the local build, warning scan, local/distributed smoke tests,
# Docker/Helm/Kubernetes guards, disposable Kubernetes recovery drill,
# non-test-mode Raft smoke, short soak, StorageD failover, GraphD failover, TLS,
# mTLS, and git diff whitespace check. This is a single entry point for local
# pre-release verification.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
BUILD_LOG="${CEDAR_PREFLIGHT_BUILD_LOG:-/tmp/cedar_release_preflight_build.log}"
SOAK_SECONDS="${CEDAR_RELEASE_SOAK_SECONDS:-15}"
SOAK_POLL_SECONDS="${CEDAR_RELEASE_SOAK_POLL_SECONDS:-3}"
FULL_CTEST="${CEDAR_RELEASE_FULL_CTEST:-0}"
SKIP_NON_TEST_RAFT="${CEDAR_RELEASE_SKIP_NON_TEST_RAFT:-0}"
TLS_SMOKE="${CEDAR_RELEASE_TLS_SMOKE:-1}"
MTLS_SMOKE="${CEDAR_RELEASE_MTLS_SMOKE:-1}"
COMPOSE_TLS_SMOKE="${CEDAR_RELEASE_COMPOSE_TLS_SMOKE:-1}"
SKIP_K8S_API="${CEDAR_RELEASE_SKIP_K8S_API:-0}"
K8S_RECOVERY_DRILL="${CEDAR_RELEASE_K8S_RECOVERY_DRILL:-1}"
BUILD_DIAGNOSTIC_PATTERN='warning:|ld: warning|macro redefined|deprecated|duplicate librar|/opt/homebrew/include/butil|error:'

validate_bool() {
    local name="$1"
    local value="$2"
    if [[ "${value}" != "0" && "${value}" != "1" ]]; then
        echo "${name} must be 0 or 1, got ${value}" >&2
        return 1
    fi
}

validate_positive_int() {
    local name="$1"
    local value="$2"
    if [[ ! "${value}" =~ ^[0-9]+$ ]] || [ "${value}" -lt 1 ]; then
        echo "${name} must be a positive integer, got ${value}" >&2
        return 1
    fi
}

run_self_test() {
    local tmp_dir
    tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/cedar-release-gate-selftest.XXXXXX")"
    trap 'rm -rf "${tmp_dir}"' RETURN

    validate_bool CEDAR_RELEASE_FULL_CTEST 0
    validate_bool CEDAR_RELEASE_FULL_CTEST 1
    validate_bool CEDAR_RELEASE_K8S_RECOVERY_DRILL 1
    validate_positive_int CEDAR_RELEASE_SOAK_SECONDS 1
    validate_positive_int CEDAR_RELEASE_SOAK_POLL_SECONDS 1
    if validate_bool CEDAR_RELEASE_K8S_RECOVERY_DRILL maybe >/dev/null 2>&1; then
        echo "self-test failed: invalid release bool should fail" >&2
        exit 1
    fi
    if validate_positive_int CEDAR_RELEASE_SOAK_SECONDS 0 >/dev/null 2>&1; then
        echo "self-test failed: zero soak seconds should fail" >&2
        exit 1
    fi
    if validate_positive_int CEDAR_RELEASE_SOAK_POLL_SECONDS -1 >/dev/null 2>&1; then
        echo "self-test failed: negative soak poll seconds should fail" >&2
        exit 1
    fi

    local bad_log="${tmp_dir}/bad-build.log"
    cat > "${bad_log}" <<'EOF'
ld: warning: ignoring duplicate library: -lprotobuf
warning: macro redefined
deprecated API used
/opt/homebrew/include/butil/endpoint.h
error: simulated compiler failure
EOF
    if ! rg -n "${BUILD_DIAGNOSTIC_PATTERN}" "${bad_log}" >/dev/null; then
        echo "self-test failed: build diagnostic pattern must catch warning/error diagnostics" >&2
        exit 1
    fi

    local good_log="${tmp_dir}/good-build.log"
    cat > "${good_log}" <<'EOF'
[100%] Built target graphd
[100%] Built target storaged
EOF
    if rg -n "${BUILD_DIAGNOSTIC_PATTERN}" "${good_log}" >/dev/null; then
        echo "self-test failed: clean build log should not match diagnostic pattern" >&2
        exit 1
    fi

    echo "Release preflight gate self-test passed"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: scripts/preflight_release_gate.sh

Runs the local pre-release gate:
  build, warning scan, local smoke, manifest syntax, deployment/Docker/Helm/K8s
  static guards, Kubernetes API dry-run, disposable Kubernetes recovery drill,
  distributed smoke, non-test-mode Raft smoke, CTest guard tests, short soak,
  StorageD failover, GraphD failover, TLS, mTLS, and diff whitespace check.

Environment:
  All switch values must be 0 or 1. All duration values must be positive integers.

  CEDAR_RELEASE_FULL_CTEST=1        also run the full CTest suite
  CEDAR_RELEASE_SOAK_SECONDS=N      short soak duration, default 15
  CEDAR_RELEASE_SOAK_POLL_SECONDS=N short soak poll interval, default 3
  CEDAR_RELEASE_SKIP_NON_TEST_RAFT=1 skip non-test-mode Raft smoke
  CEDAR_RELEASE_TLS_SMOKE=0         skip TLS smoke
  CEDAR_RELEASE_MTLS_SMOKE=0        skip mTLS smoke
  CEDAR_RELEASE_COMPOSE_TLS_SMOKE=0 skip production Compose TLS smoke
  CEDAR_RELEASE_SKIP_K8S_API=1      skip all live Kubernetes API guards
  CEDAR_RELEASE_K8S_RECOVERY_DRILL=0
                                      skip disposable Kubernetes recovery drill
  CEDAR_DRILL_NAMESPACE=name        override recovery drill namespace; default
                                      is cedargraph-recovery-drill-<timestamp>
EOF
    exit 0
fi

if [[ "${1:-}" == "--self-test" ]]; then
    run_self_test
    exit 0
fi

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }

run_step() {
    local name="$1"
    shift
    log_step "${name}"
    "$@"
}

cd "${PROJECT_ROOT}"

validate_bool CEDAR_RELEASE_FULL_CTEST "${FULL_CTEST}"
validate_bool CEDAR_RELEASE_SKIP_NON_TEST_RAFT "${SKIP_NON_TEST_RAFT}"
validate_bool CEDAR_RELEASE_TLS_SMOKE "${TLS_SMOKE}"
validate_bool CEDAR_RELEASE_MTLS_SMOKE "${MTLS_SMOKE}"
validate_bool CEDAR_RELEASE_COMPOSE_TLS_SMOKE "${COMPOSE_TLS_SMOKE}"
validate_bool CEDAR_RELEASE_SKIP_K8S_API "${SKIP_K8S_API}"
validate_bool CEDAR_RELEASE_K8S_RECOVERY_DRILL "${K8S_RECOVERY_DRILL}"
validate_positive_int CEDAR_RELEASE_SOAK_SECONDS "${SOAK_SECONDS}"
validate_positive_int CEDAR_RELEASE_SOAK_POLL_SECONDS "${SOAK_POLL_SECONDS}"

log_step "Build"
cmake --build build -j4 > "${BUILD_LOG}" 2>&1

log_step "Build warning scan"
if rg -n "${BUILD_DIAGNOSTIC_PATTERN}" "${BUILD_LOG}"; then
    echo "Build log contains warning/error diagnostics: ${BUILD_LOG}" >&2
    exit 1
fi

run_step "Local smoke" "${SCRIPT_DIR}/preflight_local_smoke.sh"
run_step "Manifest syntax guard" "${SCRIPT_DIR}/preflight_manifest_syntax.sh"
run_step "Deployment manifest static guard" "${SCRIPT_DIR}/preflight_deployment_static.sh"
run_step "Docker static guard" "${SCRIPT_DIR}/preflight_docker_static.sh"
run_step "Docker image runtime guard" "${SCRIPT_DIR}/preflight_docker_image_runtime.sh"
run_step "Production Compose smoke" "${SCRIPT_DIR}/preflight_compose_smoke.sh"
if [ "${COMPOSE_TLS_SMOKE}" = "1" ]; then
run_step "Production Compose TLS smoke" env CEDAR_COMPOSE_SMOKE_TLS=1 "${SCRIPT_DIR}/preflight_compose_smoke.sh"
fi
run_step "Helm static guard" "${SCRIPT_DIR}/preflight_helm_static.sh"
if [ "${SKIP_K8S_API}" = "1" ]; then
    log_info "Skipping Kubernetes API server guards because CEDAR_RELEASE_SKIP_K8S_API=1"
else
    command -v kubectl >/dev/null 2>&1 || {
        echo "kubectl is required for the release preflight gate; set CEDAR_RELEASE_SKIP_K8S_API=1 only for documented non-production exceptions" >&2
        exit 1
    }
    run_step "Kubernetes static guard" "${SCRIPT_DIR}/preflight_k8s_static.sh"
    run_step "Kubernetes API server dry-run guard" "${SCRIPT_DIR}/preflight_k8s_server_dry_run.sh"
    if [ "${K8S_RECOVERY_DRILL}" = "1" ]; then
        run_step "Kubernetes recovery drill" env \
            CEDAR_DRILL_NAMESPACE="${CEDAR_DRILL_NAMESPACE:-cedargraph-recovery-drill-$(date +%s)}" \
            CEDAR_DRILL_MIN_POD_AGE_SECONDS="${CEDAR_DRILL_MIN_POD_AGE_SECONDS:-0}" \
            "${SCRIPT_DIR}/preflight_k8s_recovery_drill.sh"
    else
        log_info "Skipping Kubernetes recovery drill because CEDAR_RELEASE_K8S_RECOVERY_DRILL=0"
    fi
fi
run_step "Distributed smoke" "${SCRIPT_DIR}/preflight_distributed_smoke.sh"
if [ "${SKIP_NON_TEST_RAFT}" != "1" ]; then
    run_step "Non-test-mode Raft smoke" "${SCRIPT_DIR}/preflight_non_test_raft_smoke.sh"
fi
run_step "CTest guard tests" ctest --test-dir build -R "(DisabledTests|StorageMetricsCollector)" --output-on-failure
if [ "${FULL_CTEST}" = "1" ]; then
    run_step "Full CTest" ctest --test-dir build --output-on-failure
fi
run_step "Short soak" env CEDAR_SOAK_SECONDS="${SOAK_SECONDS}" CEDAR_SOAK_POLL_SECONDS="${SOAK_POLL_SECONDS}" "${SCRIPT_DIR}/preflight_soak.sh"
run_step "StorageD failover smoke" "${SCRIPT_DIR}/preflight_failover_smoke.sh"
run_step "GraphD failover smoke" "${SCRIPT_DIR}/preflight_graphd_failover_smoke.sh"
if [ "${TLS_SMOKE}" = "1" ]; then
    run_step "TLS smoke" "${SCRIPT_DIR}/preflight_tls_smoke.sh"
fi
if [ "${MTLS_SMOKE}" = "1" ]; then
    run_step "mTLS smoke" env CEDAR_TLS_SMOKE_MTLS=1 "${SCRIPT_DIR}/preflight_tls_smoke.sh"
fi
run_step "Diff whitespace check" git diff --check

log_info "Release preflight gate passed"
