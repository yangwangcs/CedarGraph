#!/bin/bash
# =============================================================================
# CedarGraph Controlled Failover Preflight
# =============================================================================
# Starts the distributed test-mode topology, terminates one StorageD via its PID
# file, waits for MetaD heartbeat timeout detection, verifies remaining services
# stay observable, scans logs for severe diagnostics, and stops cleanly.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ID="${CEDAR_PREFLIGHT_RUN_ID:-$$}"
CLUSTER_DIR="${CEDAR_FAILOVER_PREFLIGHT_DIR:-/tmp/cedar/failover-preflight-${RUN_ID}}"
WAIT_SECONDS="${CEDAR_FAILOVER_WAIT_SECONDS:-15}"
SCAN_LOG="${CLUSTER_DIR}/log_scan.txt"
DETECTION_LOG="${CLUSTER_DIR}/failover_detection.txt"

export CEDAR_CLUSTER_DIR="${CLUSTER_DIR}"
export CEDAR_METAD_BASE_PORT="${CEDAR_METAD_BASE_PORT:-36001}"
export CEDAR_METAD_GRPC_BASE_PORT="${CEDAR_METAD_GRPC_BASE_PORT:-37001}"
export CEDAR_STORAGED_BASE_PORT="${CEDAR_STORAGED_BASE_PORT:-38779}"
export CEDAR_STORAGED_HEALTH_BASE_PORT="${CEDAR_STORAGED_HEALTH_BASE_PORT:-38879}"
export CEDAR_STORAGED_METRICS_BASE_PORT="${CEDAR_STORAGED_METRICS_BASE_PORT:-38979}"
export CEDAR_GRAPHD_PORT="${CEDAR_GRAPHD_PORT:-39679}"
export CEDAR_GRAPHD_HEALTH_PORT="${CEDAR_GRAPHD_HEALTH_PORT:-39678}"
export CEDAR_GRAPHD_METRICS_PORT="${CEDAR_GRAPHD_METRICS_PORT:-39677}"
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

scan_logs() {
    local pattern='(^|[^[:alpha:]])(FATAL|fatal|panic|segmentation fault|AddressSanitizer|UndefinedBehaviorSanitizer|Check failed|ERROR|error:)'
    if rg -n "${pattern}" "${CLUSTER_DIR}"/*.log >"${SCAN_LOG}" 2>/dev/null; then
        log_error "Runtime log scan found severe diagnostics:"
        cat "${SCAN_LOG}"
        return 1
    fi
}

check_pids() {
    local expected=$((CEDAR_METAD_COUNT + CEDAR_STORAGED_COUNT + 1))
    local actual
    actual=$(find "${CLUSTER_DIR}" -maxdepth 1 -name '*.pid' -type f | wc -l | tr -d ' ')
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

validate_positive_int CEDAR_FAILOVER_WAIT_SECONDS "${WAIT_SECONDS}"
validate_min_int CEDAR_METAD_COUNT "${CEDAR_METAD_COUNT}" 3
validate_min_int CEDAR_STORAGED_COUNT "${CEDAR_STORAGED_COUNT}" 3

rm -rf "${CLUSTER_DIR}"
mkdir -p "${CLUSTER_DIR}"

log_info "Starting failover smoke cluster in ${CLUSTER_DIR}"
"${SCRIPT_DIR}/start_distributed.sh" start
check_pids

victim_pidfile="${CLUSTER_DIR}/storaged-2.pid"
victim_pid="$(cat "${victim_pidfile}")"
log_warn "Terminating StorageD-1 [PID=${victim_pid}] to simulate node failure"
kill "${victim_pid}"
for _ in $(seq 1 100); do
    kill -0 "${victim_pid}" 2>/dev/null || break
    sleep 0.1
done
if kill -0 "${victim_pid}" 2>/dev/null; then
    log_error "StorageD-1 did not stop after SIGTERM"
    exit 1
fi
rm -f "${victim_pidfile}"

log_info "Waiting ${WAIT_SECONDS}s for MetaD heartbeat timeout processing"
sleep "${WAIT_SECONDS}"

for pidfile in "${CLUSTER_DIR}"/metad-*.pid "${CLUSTER_DIR}/storaged-1.pid" "${CLUSTER_DIR}/storaged-3.pid" "${CLUSTER_DIR}/graphd-1.pid"; do
    pid="$(cat "${pidfile}")"
    if ! kill -0 "${pid}" 2>/dev/null; then
        log_error "$(basename "${pidfile}") stopped unexpectedly"
        exit 1
    fi
done

check_http_reachable "http://127.0.0.1:${CEDAR_STORAGED_HEALTH_BASE_PORT}/health"
check_http_ok "http://127.0.0.1:${CEDAR_STORAGED_METRICS_BASE_PORT}/metrics"
check_http_reachable "http://127.0.0.1:$((CEDAR_STORAGED_HEALTH_BASE_PORT + 2))/health"
check_http_ok "http://127.0.0.1:$((CEDAR_STORAGED_METRICS_BASE_PORT + 2))/metrics"
check_http_reachable "http://127.0.0.1:${CEDAR_GRAPHD_HEALTH_PORT}/health"
check_http_ok "http://127.0.0.1:${CEDAR_GRAPHD_METRICS_PORT}/metrics"

if ! rg -n "Node .* marked as OFFLINE|heartbeat timeout" "${CLUSTER_DIR}"/metad-*.log >"${DETECTION_LOG}" 2>/dev/null; then
    log_error "MetaD did not log heartbeat timeout/offline detection"
    exit 1
fi

scan_logs

log_info "Stopping remaining failover smoke services"
stop_output="$("${SCRIPT_DIR}/start_distributed.sh" stop 2>&1)"
echo "${stop_output}"
if echo "${stop_output}" | rg -n "SIGKILL|did not exit" >/dev/null; then
    log_error "Failover smoke failed: at least one remaining service required SIGKILL"
    exit 1
fi

log_info "Failover preflight smoke passed"
