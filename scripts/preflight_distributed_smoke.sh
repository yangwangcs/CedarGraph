#!/bin/bash
# =============================================================================
# CedarGraph Distributed Preflight Smoke Test
# =============================================================================
# Starts the test-mode distributed topology through start_distributed.sh in an
# isolated directory and port range, validates status and logs, then verifies all
# processes can stop without SIGKILL.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
RUN_ID="${CEDAR_PREFLIGHT_RUN_ID:-$$}"
CLUSTER_DIR="${CEDAR_DISTRIBUTED_PREFLIGHT_DIR:-/tmp/cedar/distributed-preflight-${RUN_ID}}"
SCAN_LOG="${CLUSTER_DIR}/log_scan.txt"

export CEDAR_CLUSTER_DIR="${CLUSTER_DIR}"
export CEDAR_METAD_BASE_PORT="${CEDAR_METAD_BASE_PORT:-16001}"
export CEDAR_METAD_GRPC_BASE_PORT="${CEDAR_METAD_GRPC_BASE_PORT:-17001}"
export CEDAR_STORAGED_BASE_PORT="${CEDAR_STORAGED_BASE_PORT:-18779}"
export CEDAR_STORAGED_HEALTH_BASE_PORT="${CEDAR_STORAGED_HEALTH_BASE_PORT:-18879}"
export CEDAR_STORAGED_METRICS_BASE_PORT="${CEDAR_STORAGED_METRICS_BASE_PORT:-18979}"
export CEDAR_GRAPHD_PORT="${CEDAR_GRAPHD_PORT:-19679}"
export CEDAR_GRAPHD_HEALTH_PORT="${CEDAR_GRAPHD_HEALTH_PORT:-19678}"
export CEDAR_GRAPHD_METRICS_PORT="${CEDAR_GRAPHD_METRICS_PORT:-19677}"
export CEDAR_METAD_COUNT="${CEDAR_METAD_COUNT:-3}"
export CEDAR_STORAGED_COUNT="${CEDAR_STORAGED_COUNT:-3}"
export CEDAR_GRPC_ALLOW_INSECURE="${CEDAR_GRPC_ALLOW_INSECURE:-1}"

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

rm -rf "${CLUSTER_DIR}"
mkdir -p "${CLUSTER_DIR}"

log_info "Starting distributed smoke cluster in ${CLUSTER_DIR}"
"${SCRIPT_DIR}/start_distributed.sh" start

log_info "Checking cluster status"
"${SCRIPT_DIR}/start_distributed.sh" status

expected_pids=$((CEDAR_METAD_COUNT + CEDAR_STORAGED_COUNT + 1))
actual_pids=$(find "${CLUSTER_DIR}" -name '*.pid' -type f | wc -l | tr -d ' ')
if [ "${actual_pids}" -ne "${expected_pids}" ]; then
    log_error "Expected ${expected_pids} pid files, found ${actual_pids}"
    exit 1
fi

for pidfile in "${CLUSTER_DIR}"/*.pid; do
    pid="$(cat "${pidfile}")"
    if ! kill -0 "${pid}" 2>/dev/null; then
        log_error "$(basename "${pidfile}") is not running"
        exit 1
    fi
done

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

for i in $(seq 0 $((CEDAR_STORAGED_COUNT - 1))); do
    check_http_reachable "http://127.0.0.1:$((CEDAR_STORAGED_HEALTH_BASE_PORT + i))/health"
    check_http_ok "http://127.0.0.1:$((CEDAR_STORAGED_METRICS_BASE_PORT + i))/metrics"
done
check_http_reachable "http://127.0.0.1:${CEDAR_GRAPHD_HEALTH_PORT}/health"
check_http_ok "http://127.0.0.1:${CEDAR_GRAPHD_METRICS_PORT}/metrics"

pattern='(^|[^[:alpha:]])(FATAL|fatal|panic|segmentation fault|AddressSanitizer|UndefinedBehaviorSanitizer|Check failed|ERROR|error:)'
if rg -n "${pattern}" "${CLUSTER_DIR}"/*.log >"${SCAN_LOG}" 2>/dev/null; then
    log_error "Runtime log scan found severe diagnostics:"
    cat "${SCAN_LOG}"
    exit 1
fi

log_info "Stopping distributed smoke cluster"
stop_output="$("${SCRIPT_DIR}/start_distributed.sh" stop 2>&1)"
echo "${stop_output}"
if echo "${stop_output}" | rg -n "SIGKILL|did not exit" >/dev/null; then
    log_error "Distributed smoke failed: at least one service required SIGKILL"
    exit 1
fi

log_info "Distributed preflight smoke passed"
