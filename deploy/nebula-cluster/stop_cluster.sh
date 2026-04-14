#!/bin/bash
#
# Stop CedarGraph Nebula-Style Cluster
#

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

BASE_DIR="/tmp/cedar_nebula_cluster"
PID_DIR="$BASE_DIR/pids"

stop_service() {
    local name=$1
    local pid_file=$2
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 $pid 2>/dev/null; then
            echo -e "${YELLOW}Stopping $name (PID: $pid)...${NC}"
            kill $pid 2>/dev/null || true
            sleep 1
            # Force kill if still running
            if kill -0 $pid 2>/dev/null; then
                kill -9 $pid 2>/dev/null || true
            fi
            echo -e "${GREEN}✓ $name stopped${NC}"
        else
            echo -e "${YELLOW}$name already stopped${NC}"
        fi
        rm -f "$pid_file"
    else
        echo -e "${YELLOW}$name not running${NC}"
    fi
}

echo -e "${YELLOW}Stopping CedarGraph Nebula Cluster...${NC}"
echo ""

# Stop in reverse order
for i in 2 1 0; do
    stop_service "StorageD-$i" "$PID_DIR/storaged$i.pid"
done

stop_service "GraphD" "$PID_DIR/graphd.pid"
stop_service "MetaD" "$PID_DIR/metad.pid"

echo ""
echo -e "${GREEN}All services stopped${NC}"
