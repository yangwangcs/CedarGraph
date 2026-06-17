#!/bin/bash
# CedarGraph Local Startup Script
# 在本地启动 CedarGraph 服务

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
BUILD_DIR="${PROJECT_ROOT}/build"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# 检查二进制文件
check_binaries() {
    log_info "检查二进制文件..."
    
    if [ ! -f "${BUILD_DIR}/cedar-metad" ]; then
        log_error "cedar-metad 不存在，请先编译"
        exit 1
    fi
    
    if [ ! -f "${BUILD_DIR}/cedar-storaged" ]; then
        log_error "cedar-storaged 不存在，请先编译"
        exit 1
    fi
    
    if [ ! -f "${BUILD_DIR}/cedar-graphd" ]; then
        log_error "cedar-graphd 不存在，请先编译"
        exit 1
    fi
    
    log_info "二进制文件检查通过"
}

# 创建数据目录
create_dirs() {
    log_info "创建数据目录..."
    
    mkdir -p data/{meta0,meta1,meta2,storage0,storage1,storage2,graphd}
    mkdir -p logs/{meta0,meta1,meta2,storage0,storage1,storage2,graphd}
    
    log_info "目录创建完成"
}

# 启动 MetaD (单节点 test_mode)
start_metad() {
    local node_id=$1
    local port=$2
    
    log_info "启动 MetaD-${node_id} (端口: ${port})..."
    
    "${BUILD_DIR}/cedar-metad" \
        --node_id "${node_id}" \
        --listen "127.0.0.1:${port}" \
        --advertise "127.0.0.1:${port}" \
        --data_dir "data/meta${node_id}" \
        --test_mode \
        > "logs/meta${node_id}/metad.log" 2>&1 &
    
    echo $! > "logs/meta${node_id}/metad.pid"
    
    log_info "MetaD-${node_id} 已启动 (PID: $!)"
}

# 启动 StorageD
start_storaged() {
    local node_id=$1
    local port=$2
    
    log_info "启动 StorageD-${node_id} (端口: ${port})..."
    
    "${BUILD_DIR}/cedar-storaged" \
        --node_id "${node_id}" \
        --port "${port}" \
        --bind "127.0.0.1" \
        --advertise_address "127.0.0.1" \
        --data_dir "data/storage${node_id}" \
        --meta "127.0.0.1:9559" \
        --test_mode \
        > "logs/storage${node_id}/storaged.log" 2>&1 &
    
    echo $! > "logs/storage${node_id}/storaged.pid"
    
    log_info "StorageD-${node_id} 已启动 (PID: $!)"
}

# 启动 GraphD
start_graphd() {
    log_info "启动 GraphD (端口: 9669)..."
    
    CEDAR_TXN_WAL_DIR="data/graphd/txn_wal" \
    "${BUILD_DIR}/cedar-graphd" \
        --port 9669 \
        --bind "127.0.0.1" \
        --meta "127.0.0.1:9559" \
        --gcn "127.0.0.1:9780" \
        --test_mode \
        > "logs/graphd/graphd.log" 2>&1 &
    
    echo $! > "logs/graphd/graphd.pid"
    
    log_info "GraphD 已启动 (PID: $!)"
}

# 停止所有服务
stop_all() {
    log_info "停止所有服务..."
    
    for pidfile in logs/*/metad.pid logs/*/storaged.pid logs/graphd/graphd.pid; do
        if [ -f "$pidfile" ]; then
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid"
                log_info "停止进程 $pid"
            fi
            rm -f "$pidfile"
        fi
    done
    
    log_info "所有服务已停止"
}

# 查看状态
show_status() {
    log_info "服务状态:"
    
    for pidfile in logs/*/metad.pid logs/*/storaged.pid logs/graphd/graphd.pid; do
        if [ -f "$pidfile" ]; then
            pid=$(cat "$pidfile")
            name=$(basename "$(dirname "$pidfile")")
            if kill -0 "$pid" 2>/dev/null; then
                echo "  ${name}: 运行中 (PID: ${pid})"
            else
                echo "  ${name}: 已停止"
            fi
        fi
    done
}

# 主函数
main() {
    local command=${1:-"help"}
    
    case $command in
        start)
            check_binaries
            create_dirs
            start_metad 0 9559
            sleep 2
            start_storaged 0 9779
            sleep 2
            start_graphd
            echo ""
            show_status
            echo ""
            log_info "CedarGraph 集群已启动"
            log_info "GraphD: http://127.0.0.1:9669"
            log_info "MetaD: http://127.0.0.1:9559"
            log_info "StorageD: http://127.0.0.1:9779"
            ;;
        stop)
            stop_all
            ;;
        status)
            show_status
            ;;
        restart)
            stop_all
            sleep 2
            check_binaries
            create_dirs
            start_metad 0 9559
            sleep 2
            start_storaged 0 9779
            sleep 2
            start_graphd
            echo ""
            show_status
            ;;
        help|*)
            echo "CedarGraph 本地启动脚本"
            echo ""
            echo "用法: $0 <command>"
            echo ""
            echo "命令:"
            echo "  start    启动集群"
            echo "  stop     停止集群"
            echo "  status   查看状态"
            echo "  restart  重启集群"
            echo "  help     显示帮助"
            ;;
    esac
}

main "$@"
