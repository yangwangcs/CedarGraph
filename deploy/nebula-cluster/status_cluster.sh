#!/bin/bash
#
# Check CedarGraph Nebula-Style Cluster Status
#

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

BASE_DIR="/tmp/cedar_nebula_cluster"
PID_DIR="$BASE_DIR/pids"

METAD_PORT=9559
GRAPHD_PORT=9669
STORAGED_PORTS=(9779 9780 9781)
GCN_PORT=9782

check_service() {
    local name=$1
    local pid_file=$2
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 $pid 2>/dev/null; then
            echo -e "${GREEN}●${NC} $name (PID: $pid)"
            return 0
        else
            echo -e "${RED}✗${NC} $name (dead)"
            return 1
        fi
    else
        echo -e "${RED}✗${NC} $name (not running)"
        return 1
    fi
}

check_port() {
    local port=$1
    local name=$2
    
    if nc -z 127.0.0.1 $port 2>/dev/null; then
        echo -e "  ${GREEN}✓${NC} Port $port is listening"
        return 0
    else
        echo -e "  ${RED}✗${NC} Port $port is not responding"
        return 1
    fi
}

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  CedarGraph Cluster Status${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# Check MetaD
echo -e "${BLUE}MetaD (Metadata Service)${NC}"
check_service "MetaD" "$PID_DIR/metad.pid"
check_port $METAD_PORT "MetaD"
echo ""

# Check GraphD
echo -e "${BLUE}GraphD (Query Service)${NC}"
check_service "GraphD" "$PID_DIR/graphd.pid"
check_port $GRAPHD_PORT "GraphD"
echo ""

# Check StorageD
echo -e "${BLUE}StorageD (Storage Service)${NC}"
for i in 0 1 2; do
    port=${STORAGED_PORTS[$i]}
    check_service "StorageD-$i" "$PID_DIR/storaged$i.pid"
    check_port $port "StorageD-$i"
done
echo ""

# Check GCN
echo -e "${BLUE}GCN (Graph Compute Node)${NC}"
check_service "GCN" "$PID_DIR/gcn.pid"
check_port $GCN_PORT "GCN"

echo ""
echo -e "${BLUE}============================================${NC}"

# Summary
running=0
total=6

check_service_silent() {
    local pid_file=$1
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 $pid 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

if check_service_silent "$PID_DIR/metad.pid"; then ((running++)); fi
if check_service_silent "$PID_DIR/graphd.pid"; then ((running++)); fi
if check_service_silent "$PID_DIR/storaged0.pid"; then ((running++)); fi
if check_service_silent "$PID_DIR/storaged1.pid"; then ((running++)); fi
if check_service_silent "$PID_DIR/storaged2.pid"; then ((running++)); fi
if check_service_silent "$PID_DIR/gcn.pid"; then ((running++)); fi

if [ $running -eq $total ]; then
    echo -e "${GREEN}All $total services are running${NC}"
elif [ $running -eq 0 ]; then
    echo -e "${RED}No services are running${NC}"
else
    echo -e "${YELLOW}$running of $total services are running${NC}"
fi

echo -e "${BLUE}============================================${NC}"
