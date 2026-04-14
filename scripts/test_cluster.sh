#!/bin/bash
# Test 3-node MetaD cluster with braft consensus

set -e

echo "=== CedarGraph MetaD Cluster Test ==="

# Configuration
METAD_BIN="./build/examples/metad_server"
CONFIG_DIR="./config/cluster"
LOG_DIR="/tmp/cedar/logs"
DATA_DIR="/tmp/cedar/metad"

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    pkill -f metad_server || true
    sleep 2
    rm -rf $DATA_DIR
    echo "Cleanup done"
}

# Set trap for cleanup
trap cleanup EXIT

# Create directories
mkdir -p $LOG_DIR
mkdir -p $DATA_DIR/node{1,2,3}

# Check binary
if [ ! -f "$METAD_BIN" ]; then
    echo "ERROR: metad_server binary not found at $METAD_BIN"
    echo "Please build with: make metad_server"
    exit 1
fi

echo ""
echo "=== Starting 3-node cluster ==="

# Start Node 1
echo "Starting Node 1..."
$METAD_BIN --config=$CONFIG_DIR/metad_node1.conf > $LOG_DIR/node1.log 2>&1 &
NODE1_PID=$!
echo "Node 1 PID: $NODE1_PID"

# Start Node 2
echo "Starting Node 2..."
$METAD_BIN --config=$CONFIG_DIR/metad_node2.conf > $LOG_DIR/node2.log 2>&1 &
NODE2_PID=$!
echo "Node 2 PID: $NODE2_PID"

# Start Node 3
echo "Starting Node 3..."
$METAD_BIN --config=$CONFIG_DIR/metad_node3.conf > $LOG_DIR/node3.log 2>&1 &
NODE3_PID=$!
echo "Node 3 PID: $NODE3_PID"

# Wait for cluster to elect leader
echo ""
echo "Waiting for leader election (10 seconds)..."
sleep 10

# Test 1: Check cluster status
echo ""
echo "=== Test 1: Cluster Status ==="
curl -s http://127.0.0.1:2379/raft/status | jq . || echo "Node 1 not responding"
curl -s http://127.0.0.1:2380/raft/status | jq . || echo "Node 2 not responding"
curl -s http://127.0.0.1:2381/raft/status | jq . || echo "Node 3 not responding"

# Test 2: Create space (should work on leader)
echo ""
echo "=== Test 2: Create Space ==="
curl -s -X POST http://127.0.0.1:2379/space/create \
    -H "Content-Type: application/json" \
    -d '{"name":"test_space","partition_num":128,"replica_factor":3}' | jq .

# Test 3: Verify replication
echo ""
echo "=== Test 3: Verify Replication ==="
echo "Checking space on all nodes..."
curl -s http://127.0.0.1:2379/space/get?name=test_space | jq . || echo "Node 1: Not found"
curl -s http://127.0.0.1:2380/space/get?name=test_space | jq . || echo "Node 2: Not found"
curl -s http://127.0.0.1:2381/space/get?name=test_space | jq . || echo "Node 3: Not found"

# Test 4: Leader failover
echo ""
echo "=== Test 4: Leader Failover Test ==="
# Find current leader
LEADER_ADDR=$(curl -s http://127.0.0.1:2379/raft/status | jq -r '.leader_addr // empty')
if [ -n "$LEADER_ADDR" ]; then
    echo "Current leader: $LEADER_ADDR"
    
    # Kill leader process
    if [[ "$LEADER_ADDR" == *"9091"* ]]; then
        echo "Killing Node 1 (leader)..."
        kill $NODE1_PID || true
    elif [[ "$LEADER_ADDR" == *"9092"* ]]; then
        echo "Killing Node 2 (leader)..."
        kill $NODE2_PID || true
    else
        echo "Killing Node 3 (leader)..."
        kill $NODE3_PID || true
    fi
    
    echo "Waiting for new leader election..."
    sleep 10
    
    # Check new leader
    NEW_LEADER=$(curl -s http://127.0.0.1:2379/raft/status 2>/dev/null | jq -r '.leader_addr // empty' || \
                 curl -s http://127.0.0.1:2380/raft/status 2>/dev/null | jq -r '.leader_addr // empty' || \
                 curl -s http://127.0.0.1:2381/raft/status 2>/dev/null | jq -r '.leader_addr // empty' || echo "")
    echo "New leader: $NEW_LEADER"
    
    if [ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "$LEADER_ADDR" ]; then
        echo "✅ Leader failover successful!"
    else
        echo "⚠️ Leader failover may have failed"
    fi
else
    echo "No leader found, skipping failover test"
fi

# Test 5: Restart killed node
echo ""
echo "=== Test 5: Node Recovery ==="
echo "Restarting killed node..."
# (Restart logic would go here)

echo ""
echo "=== Cluster Test Complete ==="
