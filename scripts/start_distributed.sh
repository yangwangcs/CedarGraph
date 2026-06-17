#!/bin/bash
# =============================================================================
# CedarGraph Distributed Cluster Startup Script
# =============================================================================
# Architecture (similar to NebulaGraph):
#
#   MetaD (3 nodes, Raft consensus)
#     ├── MetaD-1: Raft:6001, gRPC:7001
#     ├── MetaD-2: Raft:6002, gRPC:7002
#     └── MetaD-3: Raft:6003, gRPC:7003
#
#   StorageD (3 nodes, register with MetaD)
#     ├── StorageD-0: gRPC:8779, Raft:8879
#     ├── StorageD-1: gRPC:8780, Raft:8880
#     └── StorageD-2: gRPC:8781, Raft:8881
#
#   GraphD (1+ nodes, query routing via MetaD)
#     └── GraphD-1: gRPC:9669
#
# Usage:
#   ./scripts/start_distributed.sh start    # Start cluster
#   ./scripts/start_distributed.sh stop     # Stop cluster
#   ./scripts/start_distributed.sh status   # Show status
#   ./scripts/start_distributed.sh restart  # Restart cluster
#   ./scripts/start_distributed.sh logs     # Tail logs
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
BUILD_DIR="${PROJECT_ROOT}/build"
CLUSTER_DIR="/tmp/cedar/cluster"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step()  { echo -e "${BLUE}[STEP]${NC} $1"; }

# MetaD configuration
METAD_COUNT=3
METAD_BASE_PORT=6001      # Raft port
METAD_GRPC_BASE_PORT=7001 # gRPC client API port

# StorageD configuration
STORAGED_COUNT=3
STORAGED_BASE_PORT=8779

# GraphD configuration
GRAPHD_PORT=9669

# =============================================================================
# Helper functions
# =============================================================================

wait_for_port() {
    local host=$1
    local port=$2
    local timeout=${3:-10}
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

check_binary() {
    local name=$1
    if [ ! -f "${BUILD_DIR}/cedar-${name}" ]; then
        log_error "cedar-${name} not found. Build first: cd build && make ${name}"
        exit 1
    fi
}

# =============================================================================
# Start functions
# =============================================================================

start_metad() {
    log_step "Starting MetaD cluster (${METAD_COUNT} nodes)..."
    
    for i in $(seq 1 $METAD_COUNT); do
        local node_id=$i
        local raft_port=$((METAD_BASE_PORT + i - 1))
        local grpc_port=$((METAD_GRPC_BASE_PORT + i - 1))
        local data_dir="${CLUSTER_DIR}/meta$((i-1))"
        local log_file="${CLUSTER_DIR}/metad-${i}.log"
        
        mkdir -p "${data_dir}"
        
        # NOTE: --test_mode bypasses braft (which has SIGSEGV on macOS ARM64).
        # In production, remove --test_mode and add --peer flags for Raft consensus.
        "${BUILD_DIR}/cedar-metad" \
            --node_id ${node_id} \
            --listen "127.0.0.1:${raft_port}" \
            --grpc_port ${grpc_port} \
            --data_dir "${data_dir}" \
            --test_mode \
            > "${log_file}" 2>&1 &
        
        local pid=$!
        echo $pid > "${CLUSTER_DIR}/metad-${i}.pid"
        
        if wait_for_port 127.0.0.1 ${grpc_port} 10; then
            log_info "  MetaD-${node_id}: Raft=${raft_port}, gRPC=${grpc_port} [PID=${pid}] ✓"
        else
            log_error "  MetaD-${node_id}: failed to start on port ${grpc_port}"
        fi
    done
}

start_storaged() {
    log_step "Starting StorageD cluster (${STORAGED_COUNT} nodes)..."
    
    # Use localhost for MetaD gRPC (IPv6 compatible on macOS)
    local meta_addr="localhost:${METAD_GRPC_BASE_PORT}"
    
    for i in $(seq 1 $STORAGED_COUNT); do
        local node_id=$((i - 1))
        local port=$((STORAGED_BASE_PORT + i - 1))
        local data_dir="${CLUSTER_DIR}/storage$((i-1))"
        local log_file="${CLUSTER_DIR}/storaged-${i}.log"
        
        mkdir -p "${data_dir}"
        
        "${BUILD_DIR}/cedar-storaged" \
            --node_id ${node_id} \
            --port ${port} \
            --bind "127.0.0.1" \
            --data_dir "${data_dir}" \
            --meta "${meta_addr}" \
            --test_mode \
            > "${log_file}" 2>&1 &
        
        local pid=$!
        echo $pid > "${CLUSTER_DIR}/storaged-${i}.pid"
        
        if wait_for_port 127.0.0.1 ${port} 10; then
            log_info "  StorageD-${node_id}: port=${port} [PID=${pid}] ✓"
        else
            log_error "  StorageD-${node_id}: failed to start on port ${port}"
        fi
    done
}

start_graphd() {
    log_step "Starting GraphD..."
    
    local meta_addr="localhost:${METAD_GRPC_BASE_PORT}"
    local log_file="${CLUSTER_DIR}/graphd-1.log"
    
    mkdir -p "${CLUSTER_DIR}/graphd"
    
    CEDAR_TXN_WAL_DIR="${CLUSTER_DIR}/graphd/txn_wal" \
    "${BUILD_DIR}/cedar-graphd" \
        --port ${GRAPHD_PORT} \
        --bind "127.0.0.1" \
        --meta "${meta_addr}" \
        --test_mode \
        > "${log_file}" 2>&1 &
    
    local pid=$!
    echo $pid > "${CLUSTER_DIR}/graphd-1.pid"
    
    if wait_for_port 127.0.0.1 ${GRAPHD_PORT} 10; then
        log_info "  GraphD-1: port=${GRAPHD_PORT} [PID=${pid}] ✓"
    else
        log_error "  GraphD-1: failed to start on port ${GRAPHD_PORT}"
    fi
}

# =============================================================================
# Stop function
# =============================================================================

stop_all() {
    log_step "Stopping all services..."
    
    for pidfile in "${CLUSTER_DIR}"/*.pid; do
        if [ -f "$pidfile" ]; then
            local pid=$(cat "$pidfile")
            local name=$(basename "$pidfile" .pid)
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null
                log_info "  Stopped ${name} [PID=${pid}]"
            fi
            rm -f "$pidfile"
        fi
    done
    
    # Clean up any remaining processes
    pkill -f "cedar-metad" 2>/dev/null || true
    pkill -f "cedar-storaged" 2>/dev/null || true
    pkill -f "cedar-graphd" 2>/dev/null || true
    
    log_info "All services stopped"
}

# =============================================================================
# Status function
# =============================================================================

show_status() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║             CedarGraph Cluster Status                       ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
    
    # MetaD status
    echo "  MetaD Nodes:"
    for i in $(seq 1 $METAD_COUNT); do
        local raft_port=$((METAD_BASE_PORT + i - 1))
        local grpc_port=$((METAD_GRPC_BASE_PORT + i - 1))
        local pidfile="${CLUSTER_DIR}/metad-${i}.pid"
        if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
            echo "    MetaD-${i}: Raft=:${raft_port}  gRPC=:${grpc_port}  RUNNING ✓"
        else
            echo "    MetaD-${i}: Raft=:${raft_port}  gRPC=:${grpc_port}  STOPPED ✗"
        fi
    done
    echo ""
    
    # StorageD status
    echo "  StorageD Nodes:"
    for i in $(seq 1 $STORAGED_COUNT); do
        local port=$((STORAGED_BASE_PORT + i - 1))
        local pidfile="${CLUSTER_DIR}/storaged-${i}.pid"
        if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
            echo "    StorageD-$((i-1)): port=:${port}  RUNNING ✓"
        else
            echo "    StorageD-$((i-1)): port=:${port}  STOPPED ✗"
        fi
    done
    echo ""
    
    # GraphD status
    echo "  GraphD Nodes:"
    local pidfile="${CLUSTER_DIR}/graphd-1.pid"
    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "    GraphD-1: port=:${GRAPHD_PORT}  RUNNING ✓"
    else
        echo "    GraphD-1: port=:${GRAPHD_PORT}  STOPPED ✗"
    fi
    echo ""
    
    echo "  Endpoints:"
    echo "    GraphD (query):   http://127.0.0.1:${GRAPHD_PORT}"
    echo "    MetaD (admin):    http://127.0.0.1:${METAD_GRPC_BASE_PORT}"
    echo ""
}

# =============================================================================
# Logs function
# =============================================================================

show_logs() {
    local service=${1:-"all"}
    case $service in
        metad*)    tail -f "${CLUSTER_DIR}"/metad-*.log ;;
        storaged*) tail -f "${CLUSTER_DIR}"/storaged-*.log ;;
        graphd*)   tail -f "${CLUSTER_DIR}"/graphd-*.log ;;
        *)         tail -f "${CLUSTER_DIR}"/*.log ;;
    esac
}

# =============================================================================
# Main
# =============================================================================

main() {
    local cmd=${1:-"help"}
    
    case $cmd in
        start)
            check_binary "metad"
            check_binary "storaged"
            check_binary "graphd"
            
            mkdir -p "${CLUSTER_DIR}"
            
            echo ""
            echo "╔══════════════════════════════════════════════════════════════╗"
            echo "║           CedarGraph Distributed Cluster Launcher           ║"
            echo "╚══════════════════════════════════════════════════════════════╝"
            echo ""
            
            start_metad
            sleep 2
            start_storaged
            sleep 2
            start_graphd
            
            echo ""
            show_status
            log_info "Cluster started! Connect with: cedar-cli -h 127.0.0.1 -p ${GRAPHD_PORT}"
            ;;
            
        stop)
            stop_all
            ;;
            
        restart)
            stop_all
            sleep 2
            main start
            ;;
            
        status)
            show_status
            ;;
            
        logs)
            show_logs "$2"
            ;;
            
        help|*)
            echo "CedarGraph Distributed Cluster Manager"
            echo ""
            echo "Usage: $0 <command>"
            echo ""
            echo "Commands:"
            echo "  start     Start the distributed cluster"
            echo "  stop      Stop all services"
            echo "  restart   Restart the cluster"
            echo "  status    Show cluster status"
            echo "  logs      Tail all logs (or: logs metad|storaged|graphd)"
            echo "  help      Show this help"
            echo ""
            echo "Architecture:"
            echo "  MetaD    (3 nodes) - Raft consensus, metadata management"
            echo "  StorageD (3 nodes) - Data storage, partition management"
            echo "  GraphD   (1 node)  - Query routing, Cypher execution"
            ;;
    esac
}

main "$@"
