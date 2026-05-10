#!/bin/bash
#
# CedarGraph Nebula-Style Cluster Deployment Script
# =================================================
# Deploys MetaD + GraphD + StorageD in a 3-tier architecture
#
# Architecture:
#   - MetaD: 1 node @ 127.0.0.1:9559 (metadata service)
#   - GraphD: 1 node @ 127.0.0.1:9669 (query service)
#   - StorageD: 3 nodes @ 127.0.0.1:9779-9781 (storage service)
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Base directories
BASE_DIR="/tmp/cedar_nebula_cluster"
LOG_DIR="$BASE_DIR/logs"
DATA_DIR="$BASE_DIR/data"
PID_DIR="$BASE_DIR/pids"

# Ports
METAD_PORT=9559
GRAPHD_PORT=9669
STORAGED_PORTS=(9779 9780 9781)

# Build directory
BUILD_DIR="${BUILD_DIR:-$(pwd)/build}"

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  CedarGraph Nebula-Style Deployment${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up existing cluster...${NC}"
    pkill -f "metad.*--port $METAD_PORT" 2>/dev/null || true
    pkill -f "cedar-queryd.*$GRAPHD_PORT" 2>/dev/null || true
    pkill -f "storaged.*--node_id" 2>/dev/null || true
    sleep 2
    rm -rf "$BASE_DIR"
}

# Check if executables exist
check_executables() {
    if [ ! -f "$BUILD_DIR/metad" ]; then
        echo -e "${RED}Error: metad not found at $BUILD_DIR/metad${NC}"
        echo "Please build with: cd build && make metad"
        exit 1
    fi
    
    if [ ! -f "$BUILD_DIR/cedar-queryd" ]; then
        echo -e "${RED}Error: cedar-queryd not found at $BUILD_DIR/cedar-queryd${NC}"
        echo "Please build with: cd build && make cedar-queryd"
        exit 1
    fi
    
    if [ ! -f "$BUILD_DIR/storaged" ]; then
        echo -e "${RED}Error: storaged not found at $BUILD_DIR/storaged${NC}"
        echo "Please build with: cd build && make storaged"
        exit 1
    fi
    
    echo -e "${GREEN}✓ All executables found${NC}"
}

# Create directories
setup_dirs() {
    echo -e "${YELLOW}Creating directories...${NC}"
    mkdir -p "$LOG_DIR"
    mkdir -p "$DATA_DIR/metad"
    mkdir -p "$DATA_DIR/graphd"
    for i in 0 1 2; do
        mkdir -p "$DATA_DIR/storaged$i"
    done
    mkdir -p "$PID_DIR"
    echo -e "${GREEN}✓ Directories created${NC}"
}

# Start MetaD
start_metad() {
    echo -e "${YELLOW}Starting MetaD (Metadata Service)...${NC}"
    
    local log_file="$LOG_DIR/metad.log"
    local pid_file="$PID_DIR/metad.pid"
    
    "$BUILD_DIR/metad" \
        --port $METAD_PORT \
        --data_dir "$DATA_DIR/metad" \
        > "$log_file" 2>&1 &
    
    local pid=$!
    echo $pid > "$pid_file"
    
    # Wait for service to start
    sleep 2
    
    if kill -0 $pid 2>/dev/null; then
        echo -e "${GREEN}✓ MetaD started on port $METAD_PORT (PID: $pid)${NC}"
    else
        echo -e "${RED}✗ MetaD failed to start. Check log: $log_file${NC}"
        exit 1
    fi
}

# Start GraphD
start_graphd() {
    echo -e "${YELLOW}Starting GraphD (Query Service)...${NC}"
    
    local log_file="$LOG_DIR/graphd.log"
    local pid_file="$PID_DIR/graphd.pid"
    
    "$BUILD_DIR/cedar-queryd" \
        --listen="127.0.0.1:$GRAPHD_PORT" \
        --meta="127.0.0.1:$METAD_PORT" \
        > "$log_file" 2>&1 &
    
    local pid=$!
    echo $pid > "$pid_file"
    
    # Wait for service to start
    sleep 2
    
    if kill -0 $pid 2>/dev/null; then
        echo -e "${GREEN}✓ GraphD started on port $GRAPHD_PORT (PID: $pid)${NC}"
    else
        echo -e "${RED}✗ GraphD failed to start. Check log: $log_file${NC}"
        exit 1
    fi
}

# Start StorageD nodes
start_storaged() {
    echo -e "${YELLOW}Starting StorageD nodes...${NC}"
    
    for i in 0 1 2; do
        local port=${STORAGED_PORTS[$i]}
        local log_file="$LOG_DIR/storaged$i.log"
        local pid_file="$PID_DIR/storaged$i.pid"
        
        "$BUILD_DIR/storaged" \
            --node_id $i \
            --port $port \
            --data_dir "$DATA_DIR/storaged" \
            --meta="127.0.0.1:$METAD_PORT" \
            > "$log_file" 2>&1 &
        
        local pid=$!
        echo $pid > "$pid_file"
        
        # Wait for service to start
        sleep 1
        
        if kill -0 $pid 2>/dev/null; then
            echo -e "${GREEN}  ✓ StorageD-$i started on port $port (PID: $pid)${NC}"
        else
            echo -e "${RED}  ✗ StorageD-$i failed to start. Check log: $log_file${NC}"
            exit 1
        fi
    done
}

# Print status
print_status() {
    echo ""
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}  Cluster Status${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""
    echo -e "${GREEN}MetaD (Metadata Service)${NC}"
    echo "  Address: 127.0.0.1:$METAD_PORT"
    echo "  PID: $(cat $PID_DIR/metad.pid 2>/dev/null || echo 'N/A')"
    echo "  Log: $LOG_DIR/metad.log"
    echo ""
    echo -e "${GREEN}GraphD (Query Service)${NC}"
    echo "  Address: 127.0.0.1:$GRAPHD_PORT"
    echo "  PID: $(cat $PID_DIR/graphd.pid 2>/dev/null || echo 'N/A')"
    echo "  Log: $LOG_DIR/graphd.log"
    echo ""
    echo -e "${GREEN}StorageD (Storage Service)${NC}"
    for i in 0 1 2; do
        echo "  Node $i: 127.0.0.1:${STORAGED_PORTS[$i]}"
        echo "    PID: $(cat $PID_DIR/storaged$i.pid 2>/dev/null || echo 'N/A')"
        echo "    Log: $LOG_DIR/storaged$i.log"
    done
    echo ""
    echo -e "${BLUE}============================================${NC}"
    echo -e "${GREEN}All services started successfully!${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""
    echo "Usage:"
    echo "  ./stop_cluster.sh     - Stop the cluster"
    echo "  ./status_cluster.sh   - Check cluster status"
    echo ""
}

# Main
main() {
    # Handle command line arguments
    if [ "$1" = "clean" ]; then
        cleanup
        echo -e "${GREEN}Cluster cleaned up${NC}"
        exit 0
    fi
    
    # Cleanup existing cluster
    cleanup
    
    # Check prerequisites
    check_executables
    
    # Setup
    setup_dirs
    
    # Start services in order
    start_metad
    start_graphd
    start_storaged
    
    # Print status
    print_status
}

main "$@"
