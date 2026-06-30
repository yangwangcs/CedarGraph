#!/bin/bash
# =============================================================================
# CedarGraph Short Soak Preflight
# =============================================================================
# Starts the distributed test-mode topology, repeatedly probes process liveness
# and health/metrics endpoints, scans logs during the run, and verifies clean
# SIGTERM shutdown. Defaults to 30 seconds; set CEDAR_SOAK_SECONDS to extend.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ID="${CEDAR_PREFLIGHT_RUN_ID:-$$}"
SOAK_SECONDS="${CEDAR_SOAK_SECONDS:-30}"
POLL_SECONDS="${CEDAR_SOAK_POLL_SECONDS:-5}"
CLUSTER_DIR="${CEDAR_SOAK_DIR:-/tmp/cedar/soak-preflight-${RUN_ID}}"
START_LOG="${CLUSTER_DIR}/start.log"
SCAN_LOG="${CLUSTER_DIR}/log_scan.txt"

export CEDAR_CLUSTER_DIR="${CLUSTER_DIR}"
export CEDAR_METAD_BASE_PORT="${CEDAR_METAD_BASE_PORT:-26001}"
export CEDAR_METAD_GRPC_BASE_PORT="${CEDAR_METAD_GRPC_BASE_PORT:-27001}"
export CEDAR_STORAGED_BASE_PORT="${CEDAR_STORAGED_BASE_PORT:-28779}"
export CEDAR_STORAGED_HEALTH_BASE_PORT="${CEDAR_STORAGED_HEALTH_BASE_PORT:-28879}"
export CEDAR_STORAGED_METRICS_BASE_PORT="${CEDAR_STORAGED_METRICS_BASE_PORT:-28979}"
export CEDAR_GRAPHD_PORT="${CEDAR_GRAPHD_PORT:-29679}"
export CEDAR_GRAPHD_HEALTH_PORT="${CEDAR_GRAPHD_HEALTH_PORT:-29678}"
export CEDAR_GRAPHD_METRICS_PORT="${CEDAR_GRAPHD_METRICS_PORT:-29677}"
export CEDAR_METAD_COUNT="${CEDAR_METAD_COUNT:-3}"
export CEDAR_STORAGED_COUNT="${CEDAR_STORAGED_COUNT:-3}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

validate_positive_int() {
    local name="$1"
    local value="$2"
    if [[ ! "${value}" =~ ^[0-9]+$ ]] || [ "${value}" -lt 1 ]; then
        echo "${name} must be a positive integer, got ${value}" >&2
        return 1
    fi
}

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
try:
    urllib.request.urlopen(sys.argv[1], timeout=2)
except urllib.error.HTTPError:
    pass
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

check_endpoints() {
    for i in $(seq 0 $((CEDAR_STORAGED_COUNT - 1))); do
        check_http_reachable "http://127.0.0.1:$((CEDAR_STORAGED_HEALTH_BASE_PORT + i))/health"
        check_http_ok "http://127.0.0.1:$((CEDAR_STORAGED_METRICS_BASE_PORT + i))/metrics"
    done
    check_http_reachable "http://127.0.0.1:${CEDAR_GRAPHD_HEALTH_PORT}/health"
    check_http_ok "http://127.0.0.1:${CEDAR_GRAPHD_METRICS_PORT}/metrics"
}

scan_logs() {
    local pattern='(^|[^[:alpha:]])(FATAL|fatal|panic|segmentation fault|AddressSanitizer|UndefinedBehaviorSanitizer|Check failed|ERROR|error:)'
    if rg -n "${pattern}" "${CLUSTER_DIR}"/*.log >"${SCAN_LOG}" 2>/dev/null; then
        log_error "Runtime log scan found severe diagnostics:"
        cat "${SCAN_LOG}"
        return 1
    fi
}

validate_positive_int CEDAR_SOAK_SECONDS "${SOAK_SECONDS}"
validate_positive_int CEDAR_SOAK_POLL_SECONDS "${POLL_SECONDS}"
validate_min_int CEDAR_METAD_COUNT "${CEDAR_METAD_COUNT}" 1
validate_min_int CEDAR_STORAGED_COUNT "${CEDAR_STORAGED_COUNT}" 1

rm -rf "${CLUSTER_DIR}"
mkdir -p "${CLUSTER_DIR}"

log_info "Starting soak cluster in ${CLUSTER_DIR}"
"${SCRIPT_DIR}/start_distributed.sh" start >"${START_LOG}"
cat "${START_LOG}"

deadline=$((SECONDS + SOAK_SECONDS))
iteration=0
while [ "${SECONDS}" -lt "${deadline}" ]; do
    iteration=$((iteration + 1))
    check_pids
    check_endpoints
    scan_logs
    log_info "Soak poll ${iteration} passed"
    sleep "${POLL_SECONDS}"
done

check_pids
check_endpoints
scan_logs

log_info "Stopping soak cluster"
stop_output="$("${SCRIPT_DIR}/start_distributed.sh" stop 2>&1)"
echo "${stop_output}"
if echo "${stop_output}" | rg -n "SIGKILL|did not exit" >/dev/null; then
    log_error "Soak failed: at least one service required SIGKILL"
    exit 1
fi

log_info "Soak preflight passed (${SOAK_SECONDS}s)"
