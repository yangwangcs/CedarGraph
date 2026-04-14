#!/bin/bash
# Start full CedarGraph cluster: 3 MetaD + 3 StorageD nodes

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "╔════════════════════════════════════════════════════════════╗"
echo "║     CedarGraph Cluster Launcher                            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if binaries exist
if [ ! -f build/metad_server ]; then
    print_info "Building metad_server..."
    cd build && make metad_server -j4
    cd ..
fi

if [ ! -f build/storaged ]; then
    print_info "Building storaged..."
    cd build && make storaged -j4
    cd ..
fi

# Clean up old data
print_info "Cleaning up old cluster data..."
rm -rf /tmp/cedar/cluster
mkdir -p /tmp/cedar/cluster/{metad,storage}

# Create directories for each node
for i in 1 2 3; do
    mkdir -p /tmp/cedar/cluster/metad/node$i
    mkdir -p /tmp/cedar/cluster/storage/node$i
done

# Function to wait for port to be ready
wait_for_port() {
    local host=$1
    local port=$2
    local timeout=${3:-30}
    local count=0
    
    while ! nc -z $host $port 2>/dev/null; do
        sleep 0.5
        count=$((count + 1))
        if [ $count -ge $((timeout * 2)) ]; then
            return 1
        fi
    done
    return 0
}

# Function to cleanup on exit
cleanup() {
    print_warn "Shutting down cluster..."
    
    # Kill MetaD nodes
    for pid in $METAD_PIDS; do
        if kill -0 $pid 2>/dev/null; then
            kill $pid 2>/dev/null
            wait $pid 2>/dev/null
        fi
    done
    
    # Kill StorageD nodes
    for pid in $STORAGE_PIDS; do
        if kill -0 $pid 2>/dev/null; then
            kill $pid 2>/dev/null
            wait $pid 2>/dev/null
        fi
    done
    
    print_success "Cluster shutdown complete"
    exit 0
}

trap cleanup INT TERM

# Export PIDs for cleanup
METAD_PIDS=""
STORAGE_PIDS=""

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Starting MetaD Cluster (3 nodes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Start MetaD Node 1
print_info "Starting MetaD Node 1 on 127.0.0.1:6001..."
./build/metad_server --config scripts/cluster_node1.conf > /tmp/cedar/cluster/metad_node1.log 2>&1 &
METAD1_PID=$!
METAD_PIDS="$METAD_PIDS $METAD1_PID"
sleep 2

# Start MetaD Node 2
print_info "Starting MetaD Node 2 on 127.0.0.1:6002..."
./build/metad_server --config scripts/cluster_node2.conf > /tmp/cedar/cluster/metad_node2.log 2>&1 &
METAD2_PID=$!
METAD_PIDS="$METAD_PIDS $METAD2_PID"
sleep 1

# Start MetaD Node 3
print_info "Starting MetaD Node 3 on 127.0.0.1:6003..."
./build/metad_server --config scripts/cluster_node3.conf > /tmp/cedar/cluster/metad_node3.log 2>&1 &
METAD3_PID=$!
METAD_PIDS="$METAD_PIDS $METAD3_PID"
sleep 1

echo ""
print_info "Waiting for MetaD cluster election (5s)..."
sleep 5

# Check MetaD status
echo ""
echo "  MetaD Cluster Status:"
echo "  ──────────────────────────────────────────────────────"
for i in 1 2 3; do
    log_file="/tmp/cedar/cluster/metad_node$i.log"
    if grep -q "became leader" "$log_file" 2>/dev/null; then
        leader_line=$(grep "became leader" "$log_file" | tail -1)
        print_success "  Node $i: LEADER"
    elif grep -q "Event loop started" "$log_file" 2>/dev/null; then
        print_info "  Node $i: FOLLOWER"
    else
        print_warn "  Node $i: STARTING..."
    fi
done
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Starting StorageD Cluster (3 nodes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Create StorageD configs
cat > /tmp/cedar/cluster/storage_node1.conf <<EOF
node_id = 1
bind_address = 127.0.0.1:7001
data_dir = /tmp/cedar/cluster/storage/node1
metad_endpoints = 1:127.0.0.1:6001,2:127.0.0.1:6002,3:127.0.0.1:6003
io_threads = 4
worker_threads = 8
EOF

cat > /tmp/cedar/cluster/storage_node2.conf <<EOF
node_id = 2
bind_address = 127.0.0.1:7002
data_dir = /tmp/cedar/cluster/storage/node2
metad_endpoints = 1:127.0.0.1:6001,2:127.0.0.1:6002,3:127.0.0.1:6003
io_threads = 4
worker_threads = 8
EOF

cat > /tmp/cedar/cluster/storage_node3.conf <<EOF
node_id = 3
bind_address = 127.0.0.1:7003
data_dir = /tmp/cedar/cluster/storage/node3
metad_endpoints = 1:127.0.0.1:6001,2:127.0.0.1:6002,3:127.0.0.1:6003
io_threads = 4
worker_threads = 8
EOF

# Start StorageD Node 1
print_info "Starting StorageD Node 1 on 127.0.0.1:7001..."
./build/storaged --config /tmp/cedar/cluster/storage_node1.conf > /tmp/cedar/cluster/storage_node1.log 2>&1 &
STORAGE1_PID=$!
STORAGE_PIDS="$STORAGE_PIDS $STORAGE1_PID"
sleep 1

# Start StorageD Node 2
print_info "Starting StorageD Node 2 on 127.0.0.1:7002..."
./build/storaged --config /tmp/cedar/cluster/storage_node2.conf > /tmp/cedar/cluster/storage_node2.log 2>&1 &
STORAGE2_PID=$!
STORAGE_PIDS="$STORAGE_PIDS $STORAGE2_PID"
sleep 1

# Start StorageD Node 3
print_info "Starting StorageD Node 3 on 127.0.0.1:7003..."
./build/storaged --config /tmp/cedar/cluster/storage_node3.conf > /tmp/cedar/cluster/storage_node3.log 2>&1 &
STORAGE3_PID=$!
STORAGE_PIDS="$STORAGE_PIDS $STORAGE3_PID"
sleep 1

echo ""
print_info "Waiting for StorageD nodes to initialize (3s)..."
sleep 3

# Check StorageD status
echo ""
echo "  StorageD Cluster Status:"
echo "  ──────────────────────────────────────────────────────"
for i in 1 2 3; do
    log_file="/tmp/cedar/cluster/storage_node$i.log"
    if grep -q "Running" "$log_file" 2>/dev/null; then
        print_success "  Node $i: RUNNING"
    elif grep -q "initialized" "$log_file" 2>/dev/null; then
        print_info "  Node $i: INITIALIZED"
    else
        print_warn "  Node $i: STARTING..."
    fi
done
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Cluster Startup Complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  MetaD Endpoints:"
echo "    - Node 1: 127.0.0.1:6001"
echo "    - Node 2: 127.0.0.1:6002"
echo "    - Node 3: 127.0.0.1:6003"
echo ""
echo "  StorageD Endpoints:"
echo "    - Node 1: 127.0.0.1:7001"
echo "    - Node 2: 127.0.0.1:7002"
echo "    - Node 3: 127.0.0.1:7003"
echo ""
echo "  Logs:"
echo "    - MetaD:   /tmp/cedar/cluster/metad_node*.log"
echo "    - StorageD: /tmp/cedar/cluster/storage_node*.log"
echo ""
echo "  Press Ctrl+C to shutdown cluster"
echo ""

# Keep script running
wait
