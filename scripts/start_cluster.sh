#!/bin/bash
# Start a 3-node CedarGraph metad cluster for testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Clean up old data
rm -rf /tmp/cedar/cluster
mkdir -p /tmp/cedar/cluster

# Build if needed
if [ ! -f build/metad_server ]; then
    echo "Building metad_server..."
    cd build && make metad_server -j4
    cd ..
fi

echo "Starting 3-node CedarGraph metad cluster..."
echo ""

# Start node 1
./build/metad_server --config scripts/cluster_node1.conf &
PID1=$!
echo "Node 1 started (PID: $PID1) on 127.0.0.1:6001"

# Start node 2
./build/metad_server --config scripts/cluster_node2.conf &
PID2=$!
echo "Node 2 started (PID: $PID2) on 127.0.0.1:6002"

# Start node 3
./build/metad_server --config scripts/cluster_node3.conf &
PID3=$!
echo "Node 3 started (PID: $PID3) on 127.0.0.1:6003"

echo ""
echo "Cluster started! Press Ctrl+C to stop all nodes."
echo ""

# Wait for interrupt
trap "echo 'Stopping cluster...'; kill $PID1 $PID2 $PID3 2>/dev/null; exit 0" INT
wait
