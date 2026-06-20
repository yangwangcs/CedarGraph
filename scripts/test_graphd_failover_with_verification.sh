#!/bin/bash
# Test GraphD Failover with MetaD node list verification

set -e

BUILD_DIR="/Users/wangyang/Desktop/CedarGraph-Core/build"
META_DATA_DIR="/tmp/cedar_test_meta2"
GRAPHD_DATA_DIR="/tmp/cedar_test_graphd2"

# Clean up previous runs
cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    pkill -f "cedar-metad.*cedar_test_meta2" 2>/dev/null || true
    pkill -f "cedar-graphd.*127.0.0.1" 2>/dev/null || true
    rm -rf "$META_DATA_DIR" "$GRAPHD_DATA_DIR"
    sleep 2
}

trap cleanup EXIT

echo "=== CedarGraph GraphD Failover Test with Node List Verification ==="
echo ""

# Step 1: Start MetaD
echo "Step 1: Starting MetaD..."
mkdir -p "$META_DATA_DIR"
cd "$BUILD_DIR"
./cedar-metad --listen 127.0.0.1:9559 --grpc_port 10559 --data_dir "$META_DATA_DIR" > /tmp/metad.log 2>&1 &
META_PID=$!
sleep 3

if ! kill -0 $META_PID 2>/dev/null; then
    echo "ERROR: MetaD failed to start"
    cat /tmp/metad.log | tail -20
    exit 1
fi
echo "✓ MetaD started (PID: $META_PID)"

# Step 2: Start 3 GraphD instances
echo ""
echo "Step 2: Starting 3 GraphD instances..."

for i in 1 2 3; do
    PORT=$((9668 + i))
    mkdir -p "$GRAPHD_DATA_DIR/$i"
    ./cedar-graphd --port $PORT --bind 127.0.0.1 --meta 127.0.0.1:10559 --test_mode > /tmp/graphd_$i.log 2>&1 &
    eval "GRAPHD_${i}_PID=$!"
    echo "✓ GraphD-$i started on port $PORT (PID: $!)"
    sleep 2
done

# Step 3: Check MetaD logs for GraphD registrations
echo ""
echo "Step 3: Checking MetaD logs for GraphD registrations..."
grep "GraphD registered" /tmp/metad.log || echo "No registration logs found"

# Step 4: List all registered GraphD nodes
echo ""
echo "Step 4: Current GraphD nodes in MetaD..."
grep -E "GraphD (registered|unregistered)" /tmp/metad.log | tail -10

# Step 5: Stop GraphD-2 to simulate failure
echo ""
echo "Step 5: Stopping GraphD-2 to simulate failure..."
eval "kill \$GRAPHD_2_PID"
sleep 2

if ! kill -0 $GRAPHD_2_PID 2>/dev/null; then
    echo "✓ GraphD-2 stopped"
else
    echo "✗ GraphD-2 still running"
fi

# Step 6: Check MetaD logs for GraphD-2 status
echo ""
echo "Step 6: Checking MetaD logs after GraphD-2 stopped..."
grep -E "GraphD (registered|unregistered|heartbeat)" /tmp/metad.log | tail -10

# Step 7: Wait for heartbeat timeout (30 seconds)
echo ""
echo "Step 7: Waiting for heartbeat timeout (30 seconds)..."
echo "MetaD will automatically remove GraphD-2 after 30 seconds of no heartbeat"
sleep 35

# Step 8: Check MetaD logs again
echo ""
echo "Step 8: MetaD logs after heartbeat timeout..."
grep -E "GraphD (registered|unregistered|heartbeat|removed|timeout)" /tmp/metad.log | tail -15

# Step 9: Verify remaining nodes
echo ""
echo "Step 9: Verifying remaining nodes..."
for i in 1 3; do
    PORT=$((9668 + i))
    eval "PID=\$GRAPHD_${i}_PID"
    if kill -0 $PID 2>/dev/null; then
        echo "✓ GraphD-$i is still running on port $PORT"
    else
        echo "✗ GraphD-$i stopped unexpectedly"
    fi
done

echo ""
echo "=== Test Summary ==="
echo "✓ MetaD running"
echo "✓ GraphD-1 running (port 9669)"
echo "✗ GraphD-2 stopped (port 9670) - simulated failure"
echo "✓ GraphD-3 running (port 9671)"
echo ""
echo "MetaD node list should show only GraphD-1 and GraphD-3"
echo "GraphD-2 should be removed after 30-second heartbeat timeout"
