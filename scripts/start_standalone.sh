#!/bin/bash
# =============================================================================
# CedarGraph Standalone - 单机模式（3 进程）
# =============================================================================
# 类似 NebulaGraph 单机部署：1 metad + 1 storaged + 1 graphd
#
# 用法:
#   ./scripts/start_standalone.sh start   # 启动
#   ./scripts/start_standalone.sh stop    # 停止
#   ./scripts/start_standalone.sh status  # 状态
#   ./scripts/start_standalone.sh restart # 重启
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
BUILD_DIR="${PROJECT_ROOT}/build"
DATA_DIR="/tmp/cedar/standalone"
export CEDAR_GRPC_ALLOW_INSECURE="${CEDAR_GRPC_ALLOW_INSECURE:-1}"

# 端口配置（与 NebulaGraph 对齐）
META_PORT=9559
STORAGE_PORT=9779
GRAPH_PORT=9669

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

wait_for_port() {
    local port=$1
    local timeout=${2:-10}
    local count=0
    while ! lsof -i :$port -sTCP:LISTEN >/dev/null 2>&1; do
        sleep 0.5
        count=$((count + 1))
        if [ $count -ge $((timeout * 2)) ]; then
            return 1
        fi
    done
    return 0
}

start() {
    echo ""
    echo "  CedarGraph Standalone"
    echo "  ─────────────────────────────────"
    echo ""

    mkdir -p "${DATA_DIR}"/{meta,storage,graphd}

    # 1. MetaD
    local META_GRPC_PORT=$((META_PORT + 1000))
    log_info "Starting MetaD on port ${META_PORT} (gRPC: ${META_GRPC_PORT})..."
    "${BUILD_DIR}/cedar-metad" \
        --node_id 1 \
        --listen "127.0.0.1:${META_PORT}" \
        --grpc_port ${META_GRPC_PORT} \
        --data_dir "${DATA_DIR}/meta" \
        --test_mode \
        > "${DATA_DIR}/metad.log" 2>&1 &
    echo $! > "${DATA_DIR}/metad.pid"
    wait_for_port ${META_GRPC_PORT} 10 && log_info "  MetaD    ✓  port=${META_PORT} (gRPC: ${META_GRPC_PORT})" || { log_error "MetaD failed"; return 1; }

    # 2. StorageD
    log_info "Starting StorageD on port ${STORAGE_PORT}..."
    "${BUILD_DIR}/cedar-storaged" \
        --node_id 0 \
        --port ${STORAGE_PORT} \
        --bind "127.0.0.1" \
        --data_dir "${DATA_DIR}/storage" \
        --meta "localhost:${META_GRPC_PORT}" \
        --test_mode \
        > "${DATA_DIR}/storaged.log" 2>&1 &
    echo $! > "${DATA_DIR}/storaged.pid"
    wait_for_port ${STORAGE_PORT} 10 && log_info "  StorageD ✓  port=${STORAGE_PORT}" || { log_error "StorageD failed"; return 1; }

    # 3. GraphD
    log_info "Starting GraphD on port ${GRAPH_PORT}..."
    CEDAR_TXN_WAL_DIR="${DATA_DIR}/graphd/txn_wal" \
    "${BUILD_DIR}/cedar-graphd" \
        --port ${GRAPH_PORT} \
        --bind "127.0.0.1" \
        --meta "localhost:${META_GRPC_PORT}" \
        --test_mode \
        > "${DATA_DIR}/graphd.log" 2>&1 &
    echo $! > "${DATA_DIR}/graphd.pid"
    wait_for_port ${GRAPH_PORT} 10 && log_info "  GraphD   ✓  port=${GRAPH_PORT}" || { log_error "GraphD failed"; return 1; }

    echo ""
    echo "  ─────────────────────────────────"
    echo "  MetaD    : localhost:${META_PORT}  (gRPC: ${META_GRPC_PORT})"
    echo "  StorageD : localhost:${STORAGE_PORT}"
    echo "  GraphD   : localhost:${GRAPH_PORT}"
    echo "  ─────────────────────────────────"
    echo ""
    log_info "Connect: cedar-cli -h 127.0.0.1 -p ${GRAPH_PORT}"
}

stop() {
    log_info "Stopping CedarGraph..."
    for svc in metad storaged graphd; do
        pidfile="${DATA_DIR}/${svc}.pid"
        if [ -f "$pidfile" ]; then
            pid=$(cat "$pidfile")
            kill "$pid" 2>/dev/null && log_info "  Stopped ${svc} [${pid}]" || true
            rm -f "$pidfile"
        fi
    done
    log_info "Done"
}

status() {
    local META_GRPC_PORT=$((META_PORT + 1000))
    echo ""
    echo "  CedarGraph Standalone Status"
    echo "  ─────────────────────────────────"
    for svc_info in "metad:${META_GRPC_PORT}" "storaged:${STORAGE_PORT}" "graphd:${GRAPH_PORT}"; do
        local svc="${svc_info%%:*}"
        local port="${svc_info##*:}"
        local pidfile="${DATA_DIR}/${svc}.pid"
        if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
            echo "  ${svc}: port=${port} RUNNING ✓"
        else
            echo "  ${svc}: port=${port} STOPPED ✗"
        fi
    done
    echo ""
}

case ${1:-help} in
    start)   start ;;
    stop)    stop ;;
    restart) stop; sleep 1; start ;;
    status)  status ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        ;;
esac
