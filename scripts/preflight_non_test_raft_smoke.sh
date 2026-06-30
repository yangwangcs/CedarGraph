#!/bin/bash
# =============================================================================
# CedarGraph Non-Test-Mode Raft Smoke
# =============================================================================
# Starts MetaD/StorageD/GraphD without --test_mode on isolated local ports. This
# catches production-Raft integration regressions such as brpc/braft ABI drift.
# It intentionally does not bootstrap a default space; deployment/bootstrap
# tooling owns schema creation.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ID="${CEDAR_PREFLIGHT_RUN_ID:-$$}"
CLUSTER_DIR="${CEDAR_NON_TEST_RAFT_DIR:-/tmp/cedar/non-test-raft-preflight-${RUN_ID}}"
START_LOG="${CLUSTER_DIR}/start.log"
SCAN_LOG="${CLUSTER_DIR}/log_scan.txt"

export CEDAR_CLUSTER_DIR="${CLUSTER_DIR}"
export CEDAR_TEST_MODE=0
export CEDAR_TLS_ENABLED="${CEDAR_TLS_ENABLED:-false}"
export CEDAR_GRPC_ALLOW_INSECURE="${CEDAR_GRPC_ALLOW_INSECURE:-1}"
export CEDAR_METAD_BASE_PORT="${CEDAR_METAD_BASE_PORT:-56001}"
export CEDAR_METAD_GRPC_BASE_PORT="${CEDAR_METAD_GRPC_BASE_PORT:-57001}"
export CEDAR_STORAGED_BASE_PORT="${CEDAR_STORAGED_BASE_PORT:-58779}"
export CEDAR_STORAGED_HEALTH_BASE_PORT="${CEDAR_STORAGED_HEALTH_BASE_PORT:-58879}"
export CEDAR_STORAGED_METRICS_BASE_PORT="${CEDAR_STORAGED_METRICS_BASE_PORT:-58979}"
export CEDAR_GRAPHD_PORT="${CEDAR_GRAPHD_PORT:-59679}"
export CEDAR_GRAPHD_HEALTH_PORT="${CEDAR_GRAPHD_HEALTH_PORT:-59678}"
export CEDAR_GRAPHD_METRICS_PORT="${CEDAR_GRAPHD_METRICS_PORT:-59677}"
export CEDAR_METAD_COUNT="${CEDAR_METAD_COUNT:-3}"
export CEDAR_STORAGED_COUNT="${CEDAR_STORAGED_COUNT:-3}"
export CEDAR_GRAPHD_AUTH_JWT_SECRET="${CEDAR_GRAPHD_AUTH_JWT_SECRET:-cedar-preflight-jwt-secret-with-at-least-32-bytes}"
export CEDAR_GRAPHD_AUTH_USER="${CEDAR_GRAPHD_AUTH_USER:-preflight-admin}"
export CEDAR_GRAPHD_AUTH_PASSWORD="${CEDAR_GRAPHD_AUTH_PASSWORD:-preflight-password}"
export CEDAR_GRAPHD_AUTH_ROLE="${CEDAR_GRAPHD_AUTH_ROLE:-admin}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

validate_min_int() {
    local name="$1"
    local value="$2"
    local min="$3"
    if [[ ! "${value}" =~ ^[0-9]+$ ]] || [ "${value}" -lt "${min}" ]; then
        echo "${name} must be an integer >= ${min}, got ${value}" >&2
        return 1
    fi
}

cleanup() {
    set +e
    "${SCRIPT_DIR}/start_distributed.sh" stop >/dev/null 2>&1
}

trap cleanup EXIT INT TERM

validate_min_int CEDAR_METAD_COUNT "${CEDAR_METAD_COUNT}" 1
validate_min_int CEDAR_STORAGED_COUNT "${CEDAR_STORAGED_COUNT}" 1

check_http_reachable() {
    local url="$1"
    if command -v curl >/dev/null 2>&1; then
        local code
        code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "${url}")"
        [ "${code}" != "000" ]
    else
        python3 - "$url" <<'PY'
import sys
import urllib.request
urllib.request.urlopen(sys.argv[1], timeout=2)
PY
    fi
}

check_http_ok() {
    local url="$1"
    if command -v curl >/dev/null 2>&1; then
        curl -fsS --max-time 2 "${url}" >/dev/null
    else
        python3 - "$url" <<'PY'
import sys
import urllib.request
with urllib.request.urlopen(sys.argv[1], timeout=2) as response:
    if response.status >= 400:
        raise SystemExit(1)
PY
    fi
}

check_pids() {
    local expected=$((CEDAR_METAD_COUNT + CEDAR_STORAGED_COUNT + 1))
    local actual
    actual=$(find "${CLUSTER_DIR}" -name '*.pid' -type f | wc -l | tr -d ' ')
    if [ "${actual}" -ne "${expected}" ]; then
        log_error "Expected ${expected} pid files, found ${actual}"
        return 1
    fi
    for pidfile in "${CLUSTER_DIR}"/*.pid; do
        local pid
        pid="$(cat "${pidfile}")"
        if ! kill -0 "${pid}" 2>/dev/null; then
            log_error "$(basename "${pidfile}") is not running"
            return 1
        fi
    done
}

scan_logs() {
    local severe='FATAL|fatal|panic|segmentation fault|AddressSanitizer|UndefinedBehaviorSanitizer|stack-buffer-overflow|Check failed'
    if rg -n "${severe}" "${CLUSTER_DIR}"/*.log >"${SCAN_LOG}" 2>/dev/null; then
        log_error "Non-test-mode Raft log scan found severe diagnostics:"
        cat "${SCAN_LOG}"
        return 1
    fi
}

rm -rf "${CLUSTER_DIR}"
mkdir -p "${CLUSTER_DIR}"

log_info "Starting non-test-mode Raft cluster in ${CLUSTER_DIR}"
"${SCRIPT_DIR}/start_distributed.sh" start >"${START_LOG}"
cat "${START_LOG}"

check_pids
for i in $(seq 0 $((CEDAR_STORAGED_COUNT - 1))); do
    check_http_reachable "http://127.0.0.1:$((CEDAR_STORAGED_HEALTH_BASE_PORT + i))/health"
    check_http_ok "http://127.0.0.1:$((CEDAR_STORAGED_METRICS_BASE_PORT + i))/metrics"
done
check_http_reachable "http://127.0.0.1:${CEDAR_GRAPHD_HEALTH_PORT}/health"
check_http_ok "http://127.0.0.1:${CEDAR_GRAPHD_METRICS_PORT}/metrics"
scan_logs

log_warn "Default space is not bootstrapped by this smoke; schema bootstrap remains a deployment prerequisite."

stop_output="$("${SCRIPT_DIR}/start_distributed.sh" stop 2>&1)"
echo "${stop_output}"
if echo "${stop_output}" | rg -n "SIGKILL|did not exit" >/dev/null; then
    log_error "Non-test-mode Raft smoke failed: at least one service required SIGKILL"
    exit 1
fi

scan_logs
log_info "Non-test-mode Raft smoke passed"
