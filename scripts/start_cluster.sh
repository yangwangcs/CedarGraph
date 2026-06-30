#!/bin/bash
# Start a 3-node CedarGraph metad cluster for testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Clean up old data
rm -rf /tmp/cedar/cluster
mkdir -p /tmp/cedar/cluster

# Build if needed
if [ ! -f build/cedar-metad ]; then
    echo "Building cedar-metad..."
    cd build && make metad -j4
    cd ..
fi

echo "Starting 3-node CedarGraph metad cluster..."
echo ""

cleanup() {
    echo "Stopping cluster..."
    for pid in ${PID1:-} ${PID2:-} ${PID3:-}; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    wait ${PID1:-} ${PID2:-} ${PID3:-} 2>/dev/null || true
}

trap 'cleanup; exit 0' INT TERM
trap cleanup EXIT

# Start node 1
./build/cedar-metad --config scripts/cluster_node1.conf &
PID1=$!
echo "Node 1 started (PID: $PID1) on 127.0.0.1:6001"

# Start node 2
./build/cedar-metad --config scripts/cluster_node2.conf &
PID2=$!
echo "Node 2 started (PID: $PID2) on 127.0.0.1:6002"

# Start node 3
./build/cedar-metad --config scripts/cluster_node3.conf &
PID3=$!
echo "Node 3 started (PID: $PID3) on 127.0.0.1:6003"

echo ""
echo "Cluster started! Press Ctrl+C to stop all nodes."
echo ""

wait
