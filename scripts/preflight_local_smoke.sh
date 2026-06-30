#!/bin/bash
# =============================================================================
# CedarGraph Local Preflight Smoke Test
# =============================================================================
# Starts one MetaD, one StorageD and one GraphD under an isolated /tmp directory,
# verifies their listener ports, scans runtime logs for severe diagnostics, and
# stops only the PIDs created by this script.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
BUILD_DIR="${PROJECT_ROOT}/build"
RUN_ID="${CEDAR_PREFLIGHT_RUN_ID:-$$}"
DATA_DIR="${CEDAR_PREFLIGHT_DIR:-/tmp/cedar/preflight-${RUN_ID}}"
SCAN_LOG="${DATA_DIR}/log_scan.txt"

META_RAFT_PORT="${CEDAR_PREFLIGHT_META_RAFT_PORT:-19559}"
META_GRPC_PORT="${CEDAR_PREFLIGHT_META_GRPC_PORT:-20559}"
STORAGE_PORT="${CEDAR_PREFLIGHT_STORAGE_PORT:-19779}"
GRAPH_PORT="${CEDAR_PREFLIGHT_GRAPH_PORT:-19679}"
STORAGE_HEALTH_PORT="${CEDAR_PREFLIGHT_STORAGE_HEALTH_PORT:-19780}"
STORAGE_METRICS_PORT="${CEDAR_PREFLIGHT_STORAGE_METRICS_PORT:-19781}"
GRAPH_HEALTH_PORT="${CEDAR_PREFLIGHT_GRAPH_HEALTH_PORT:-19668}"
GRAPH_METRICS_PORT="${CEDAR_PREFLIGHT_GRAPH_METRICS_PORT:-19667}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

validate_port() {
    local name="$1"
    local value="$2"
    if [[ ! "${value}" =~ ^[0-9]+$ ]] || [ "${value}" -lt 1 ] || [ "${value}" -gt 65535 ]; then
        echo "${name} must be an integer port in 1..65535, got ${value}" >&2
        return 1
    fi
}

require_binary() {
    local binary="$1"
    if [ ! -x "${BUILD_DIR}/${binary}" ]; then
        log_error "${BUILD_DIR}/${binary} not found or not executable. Run: cmake --build build -j4"
        exit 1
    fi
}

wait_for_port() {
    local port="$1"
    local timeout="${2:-15}"
    local count=0
    while ! lsof -i :"${port}" -sTCP:LISTEN >/dev/null 2>&1; do
        sleep 0.5
        count=$((count + 1))
        if [ "${count}" -ge $((timeout * 2)) ]; then
            return 1
        fi
    done
    return 0
}

require_port_free() {
    local port="$1"
    if lsof -i :"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
        log_error "Port ${port} is already in use"
        return 1
    fi
}

stop_pidfile() {
    local pidfile="$1"
    local name="$2"
    local max_wait="${3:-100}"
    if [ ! -f "${pidfile}" ]; then
        return 0
    fi

    local pid
    pid="$(cat "${pidfile}")"
    if kill -0 "${pid}" 2>/dev/null; then
        kill "${pid}" 2>/dev/null || true
        for _ in $(seq 1 "${max_wait}"); do
            kill -0 "${pid}" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "${pid}" 2>/dev/null; then
            log_warn "${name} [PID=${pid}] did not exit after SIGTERM; sending SIGKILL"
            kill -9 "${pid}" 2>/dev/null || true
            rm -f "${pidfile}"
            return 2
        fi
    fi
    rm -f "${pidfile}"
    return 0
}

cleanup() {
    set +e
    stop_pidfile "${DATA_DIR}/graphd.pid" "GraphD" 30 >/dev/null 2>&1
    stop_pidfile "${DATA_DIR}/storaged.pid" "StorageD" 30 >/dev/null 2>&1
    stop_pidfile "${DATA_DIR}/metad.pid" "MetaD" 30 >/dev/null 2>&1
}

trap cleanup EXIT INT TERM

scan_logs() {
    local pattern='(^|[^[:alpha:]])(FATAL|fatal|panic|segmentation fault|AddressSanitizer|UndefinedBehaviorSanitizer|Check failed|ERROR|error:)'
    if rg -n "${pattern}" "${DATA_DIR}"/*.log >"${SCAN_LOG}" 2>/dev/null; then
        log_error "Runtime log scan found severe diagnostics:"
        cat "${SCAN_LOG}"
        return 1
    fi
}

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

require_binary cedar-metad
require_binary cedar-storaged
require_binary cedar-graphd

export CEDAR_GRPC_ALLOW_INSECURE="${CEDAR_GRPC_ALLOW_INSECURE:-1}"

validate_port CEDAR_PREFLIGHT_META_RAFT_PORT "${META_RAFT_PORT}"
validate_port CEDAR_PREFLIGHT_META_GRPC_PORT "${META_GRPC_PORT}"
validate_port CEDAR_PREFLIGHT_STORAGE_PORT "${STORAGE_PORT}"
validate_port CEDAR_PREFLIGHT_STORAGE_HEALTH_PORT "${STORAGE_HEALTH_PORT}"
validate_port CEDAR_PREFLIGHT_STORAGE_METRICS_PORT "${STORAGE_METRICS_PORT}"
validate_port CEDAR_PREFLIGHT_GRAPH_PORT "${GRAPH_PORT}"
validate_port CEDAR_PREFLIGHT_GRAPH_HEALTH_PORT "${GRAPH_HEALTH_PORT}"
validate_port CEDAR_PREFLIGHT_GRAPH_METRICS_PORT "${GRAPH_METRICS_PORT}"

rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"/{meta,storage,graphd}

for port in "${META_RAFT_PORT}" "${META_GRPC_PORT}" "${STORAGE_PORT}" \
            "${STORAGE_HEALTH_PORT}" "${STORAGE_METRICS_PORT}" \
            "${GRAPH_PORT}" "${GRAPH_HEALTH_PORT}" "${GRAPH_METRICS_PORT}"; do
    require_port_free "${port}"
done

log_info "Starting MetaD on 127.0.0.1:${META_RAFT_PORT} (gRPC ${META_GRPC_PORT})"
"${BUILD_DIR}/cedar-metad" \
    --node_id 1 \
    --listen "127.0.0.1:${META_RAFT_PORT}" \
    --grpc_port "${META_GRPC_PORT}" \
    --data_dir "${DATA_DIR}/meta" \
    --test_mode \
    > "${DATA_DIR}/metad.log" 2>&1 &
echo $! > "${DATA_DIR}/metad.pid"
if ! wait_for_port "${META_GRPC_PORT}" 15; then
    log_error "MetaD failed to listen on ${META_GRPC_PORT}"
    tail -50 "${DATA_DIR}/metad.log" || true
    exit 1
fi

log_info "Starting StorageD on 127.0.0.1:${STORAGE_PORT}"
"${BUILD_DIR}/cedar-storaged" \
    --node_id 0 \
    --port "${STORAGE_PORT}" \
    --bind "127.0.0.1" \
    --data_dir "${DATA_DIR}/storage" \
    --meta "localhost:${META_GRPC_PORT}" \
    --health_port "${STORAGE_HEALTH_PORT}" \
    --metrics_port "${STORAGE_METRICS_PORT}" \
    --test_mode \
    > "${DATA_DIR}/storaged.log" 2>&1 &
echo $! > "${DATA_DIR}/storaged.pid"
if ! wait_for_port "${STORAGE_PORT}" 15; then
    log_error "StorageD failed to listen on ${STORAGE_PORT}"
    tail -50 "${DATA_DIR}/storaged.log" || true
    exit 1
fi

log_info "Starting GraphD on 127.0.0.1:${GRAPH_PORT}"
CEDAR_TXN_WAL_DIR="${DATA_DIR}/graphd/txn_wal" \
"${BUILD_DIR}/cedar-graphd" \
    --port "${GRAPH_PORT}" \
    --bind "127.0.0.1" \
    --meta "localhost:${META_GRPC_PORT}" \
    --health_port "${GRAPH_HEALTH_PORT}" \
    --metrics_port "${GRAPH_METRICS_PORT}" \
    --test_mode \
    > "${DATA_DIR}/graphd.log" 2>&1 &
echo $! > "${DATA_DIR}/graphd.pid"
if ! wait_for_port "${GRAPH_PORT}" 15; then
    log_error "GraphD failed to listen on ${GRAPH_PORT}"
    tail -50 "${DATA_DIR}/graphd.log" || true
    exit 1
fi

sleep 2
check_http_reachable "http://127.0.0.1:${STORAGE_HEALTH_PORT}/health"
check_http_ok "http://127.0.0.1:${STORAGE_METRICS_PORT}/metrics"
check_http_reachable "http://127.0.0.1:${GRAPH_HEALTH_PORT}/health"
check_http_ok "http://127.0.0.1:${GRAPH_METRICS_PORT}/metrics"
scan_logs

shutdown_status=0
stop_pidfile "${DATA_DIR}/graphd.pid" "GraphD" 150 || shutdown_status=$?
stop_pidfile "${DATA_DIR}/storaged.pid" "StorageD" 150 || shutdown_status=$?
stop_pidfile "${DATA_DIR}/metad.pid" "MetaD" 150 || shutdown_status=$?
if [ "${shutdown_status}" -ne 0 ]; then
    log_error "Preflight smoke failed: at least one service required SIGKILL during shutdown"
    exit 1
fi

log_info "Preflight smoke passed"
log_info "Logs and data: ${DATA_DIR}"
