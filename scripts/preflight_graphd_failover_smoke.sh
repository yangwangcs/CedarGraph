#!/bin/bash
# =============================================================================
# CedarGraph GraphD Failover Preflight
# =============================================================================
# Starts one MetaD and three GraphD instances, terminates one GraphD via PID,
# waits for MetaD's GraphD cleanup loop to remove it, verifies the remaining
# GraphD instances stay observable, scans logs, and stops cleanly.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
BUILD_DIR="${CEDAR_BUILD_DIR:-${PROJECT_ROOT}/build}"
RUN_ID="${CEDAR_PREFLIGHT_RUN_ID:-$$}"
DATA_DIR="${CEDAR_GRAPHD_FAILOVER_DIR:-/tmp/cedar/graphd-failover-preflight-${RUN_ID}}"
SCAN_LOG="${DATA_DIR}/log_scan.txt"
DETECTION_LOG="${DATA_DIR}/graphd_failover_detection.txt"

META_RAFT_PORT="${CEDAR_GRAPHD_FAILOVER_META_RAFT_PORT:-46001}"
META_GRPC_PORT="${CEDAR_GRAPHD_FAILOVER_META_GRPC_PORT:-47001}"
GRAPHD_BASE_PORT="${CEDAR_GRAPHD_FAILOVER_GRAPHD_BASE_PORT:-49679}"
GRAPHD_HEALTH_BASE_PORT="${CEDAR_GRAPHD_FAILOVER_HEALTH_BASE_PORT:-49689}"
GRAPHD_METRICS_BASE_PORT="${CEDAR_GRAPHD_FAILOVER_METRICS_BASE_PORT:-49699}"
WAIT_SECONDS="${CEDAR_GRAPHD_FAILOVER_WAIT_SECONDS:-45}"

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

validate_port() {
    local name="$1"
    local value="$2"
    if [[ ! "${value}" =~ ^[0-9]+$ ]] || [ "${value}" -lt 1 ] || [ "${value}" -gt 65535 ]; then
        echo "${name} must be an integer port in 1..65535, got ${value}" >&2
        return 1
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
    if [ ! -f "${pidfile}" ]; then
        return 0
    fi
    local pid
    pid="$(cat "${pidfile}")"
    if kill -0 "${pid}" 2>/dev/null; then
        kill "${pid}" 2>/dev/null || true
        for _ in $(seq 1 150); do
            kill -0 "${pid}" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "${pid}" 2>/dev/null; then
            log_warn "${name} [PID=${pid}] did not exit after SIGTERM; sending SIGKILL"
            kill -9 "${pid}" 2>/dev/null || true
            return 2
        fi
    fi
    rm -f "${pidfile}"
}

cleanup() {
    set +e
    for pidfile in "${DATA_DIR}"/graphd-*.pid; do
        [ -f "${pidfile}" ] && stop_pidfile "${pidfile}" "$(basename "${pidfile}" .pid)" >/dev/null 2>&1
    done
    stop_pidfile "${DATA_DIR}/metad.pid" "MetaD" >/dev/null 2>&1
}

trap cleanup EXIT INT TERM

export CEDAR_GRPC_ALLOW_INSECURE="${CEDAR_GRPC_ALLOW_INSECURE:-1}"

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
    if rg -n "${pattern}" "${DATA_DIR}"/*.log >"${SCAN_LOG}" 2>/dev/null; then
        log_error "Runtime log scan found severe diagnostics:"
        cat "${SCAN_LOG}"
        return 1
    fi
}

check_pids() {
    local expected=4
    local actual
    actual=$(find "${DATA_DIR}" -maxdepth 1 -name '*.pid' -type f | wc -l | tr -d ' ')
    if [ "${actual}" -ne "${expected}" ]; then
        log_error "Expected ${expected} pid files, found ${actual}"
        return 1
    fi
    for pidfile in "${DATA_DIR}"/*.pid; do
        local pid
        pid="$(cat "${pidfile}")"
        if ! kill -0 "${pid}" 2>/dev/null; then
            log_error "$(basename "${pidfile}") is not running"
            return 1
        fi
    done
}

validate_positive_int CEDAR_GRAPHD_FAILOVER_WAIT_SECONDS "${WAIT_SECONDS}"
validate_port CEDAR_GRAPHD_FAILOVER_META_RAFT_PORT "${META_RAFT_PORT}"
validate_port CEDAR_GRAPHD_FAILOVER_META_GRPC_PORT "${META_GRPC_PORT}"
validate_port CEDAR_GRAPHD_FAILOVER_GRAPHD_BASE_PORT "${GRAPHD_BASE_PORT}"
validate_port CEDAR_GRAPHD_FAILOVER_HEALTH_BASE_PORT "${GRAPHD_HEALTH_BASE_PORT}"
validate_port CEDAR_GRAPHD_FAILOVER_METRICS_BASE_PORT "${GRAPHD_METRICS_BASE_PORT}"

rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"/{meta,graphd}

require_port_free "${META_RAFT_PORT}"
require_port_free "${META_GRPC_PORT}"
for i in 0 1 2; do
    require_port_free "$((GRAPHD_BASE_PORT + i))"
    require_port_free "$((GRAPHD_HEALTH_BASE_PORT + i))"
    require_port_free "$((GRAPHD_METRICS_BASE_PORT + i))"
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
wait_for_port "${META_GRPC_PORT}" 15 || { tail -50 "${DATA_DIR}/metad.log"; exit 1; }

for i in 0 1 2; do
    port=$((GRAPHD_BASE_PORT + i))
    health_port=$((GRAPHD_HEALTH_BASE_PORT + i))
    metrics_port=$((GRAPHD_METRICS_BASE_PORT + i))
    log_info "Starting GraphD-${i} on 127.0.0.1:${port}"
    CEDAR_TXN_WAL_DIR="${DATA_DIR}/graphd/txn_wal-${i}" \
    "${BUILD_DIR}/cedar-graphd" \
        --port "${port}" \
        --bind "127.0.0.1" \
        --meta "localhost:${META_GRPC_PORT}" \
        --health_port "${health_port}" \
        --metrics_port "${metrics_port}" \
        --test_mode \
        > "${DATA_DIR}/graphd-${i}.log" 2>&1 &
    echo $! > "${DATA_DIR}/graphd-${i}.pid"
    wait_for_port "${port}" 15 || { tail -50 "${DATA_DIR}/graphd-${i}.log"; exit 1; }
done

sleep 2
check_pids
for i in 0 1 2; do
    check_http_reachable "http://127.0.0.1:$((GRAPHD_HEALTH_BASE_PORT + i))/health"
    check_http_ok "http://127.0.0.1:$((GRAPHD_METRICS_BASE_PORT + i))/metrics"
done
scan_logs

victim_pidfile="${DATA_DIR}/graphd-1.pid"
victim_pid="$(cat "${victim_pidfile}")"
log_warn "Killing GraphD-1 [PID=${victim_pid}] to simulate abrupt query service failure"
kill -9 "${victim_pid}"
for _ in $(seq 1 100); do
    kill -0 "${victim_pid}" 2>/dev/null || break
    sleep 0.1
done
if kill -0 "${victim_pid}" 2>/dev/null; then
    log_error "GraphD-1 did not stop after SIGKILL"
    exit 1
fi
rm -f "${victim_pidfile}"

log_info "Waiting up to ${WAIT_SECONDS}s for MetaD GraphD cleanup"
detected=0
for _ in $(seq 1 "${WAIT_SECONDS}"); do
    if rg -n "GraphD node timeout, removing" "${DATA_DIR}/metad.log" >"${DETECTION_LOG}" 2>/dev/null; then
        detected=1
        break
    fi
    sleep 1
done
if [ "${detected}" -ne 1 ]; then
    log_error "MetaD did not log GraphD timeout cleanup"
    exit 1
fi

for i in 0 2; do
    pid="$(cat "${DATA_DIR}/graphd-${i}.pid")"
    if ! kill -0 "${pid}" 2>/dev/null; then
        log_error "GraphD-${i} stopped unexpectedly"
        exit 1
    fi
    check_http_reachable "http://127.0.0.1:$((GRAPHD_HEALTH_BASE_PORT + i))/health"
    check_http_ok "http://127.0.0.1:$((GRAPHD_METRICS_BASE_PORT + i))/metrics"
done
scan_logs

shutdown_status=0
for i in 0 2; do
    stop_pidfile "${DATA_DIR}/graphd-${i}.pid" "GraphD-${i}" || shutdown_status=$?
done
stop_pidfile "${DATA_DIR}/metad.pid" "MetaD" || shutdown_status=$?
if [ "${shutdown_status}" -ne 0 ]; then
    log_error "GraphD failover smoke failed: at least one service required SIGKILL"
    exit 1
fi

log_info "GraphD failover preflight smoke passed"
