#!/bin/bash
# =============================================================================
# CedarGraph Temporal Graph Docker Test Script
# 时态图 Docker 测试脚本 - 测试 3/5/7 节点时态图性能
#
# Usage:
#   ./test_temporal_docker.sh [3|5|7|all]
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="docker-compose.scalable.yml"
BENCHMARK_BIN="$SCRIPT_DIR/build/test_temporal_graph_perf"
RESULTS_DIR="$SCRIPT_DIR/test_results/temporal"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_header() { echo -e "${CYAN}$1${NC}"; }

# =============================================================================
# Prerequisites
# =============================================================================

check_prerequisites() {
  log_info "Checking prerequisites..."
  
  if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed"
    exit 1
  fi
  
  if ! docker compose version &> /dev/null; then
    log_error "Docker Compose v2 is required"
    exit 1
  fi
  
  if [ ! -f "$BENCHMARK_BIN" ]; then
    log_warning "Benchmark binary not found, will build..."
    cd "$SCRIPT_DIR/build" && cmake --build . --target test_temporal_graph_perf
  fi
  
  mkdir -p "$RESULTS_DIR"
  log_success "Prerequisites check passed"
}

# =============================================================================
# Docker Cluster Management
# =============================================================================

get_profile() {
  local count=$1
  case $count in
    3) echo "" ;;
    5) echo "--profile 5nodes" ;;
    7) echo "--profile 7nodes" ;;
    *) echo "" ;;
  esac
}

start_cluster() {
  local node_count=$1
  local profile=$(get_profile $node_count)
  
  log_header "========================================"
  log_header "Starting ${node_count}-Node Docker Cluster"
  log_header "========================================"
  
  cd "$SCRIPT_DIR"
  
  # Stop existing cluster
  docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
  sleep 2
  
  # Start new cluster
  log_info "Starting Docker containers..."
  if [ -n "$profile" ]; then
    docker compose -f "$COMPOSE_FILE" $profile up -d
  else
    docker compose -f "$COMPOSE_FILE" up -d
  fi
  
  # Wait for cluster to be ready
  log_info "Waiting for cluster to be ready..."
  local timeout=180
  local elapsed=0
  
  while [ $elapsed -lt $timeout ]; do
    local all_healthy=true
    
    # Check MetaD
    for i in 1 2 3; do
      if ! docker compose -f "$COMPOSE_FILE" ps metad$i 2>/dev/null | grep -q "Up"; then
        all_healthy=false
        break
      fi
    done
    
    # Check StorageD
    for i in $(seq 1 $node_count); do
      if ! docker compose -f "$COMPOSE_FILE" ps storaged$i 2>/dev/null | grep -q "Up"; then
        all_healthy=false
        break
      fi
    done
    
    if [ "$all_healthy" = true ]; then
      log_success "Cluster is ready!"
      return 0
    fi
    
    echo -n "."
    sleep 5
    elapsed=$((elapsed + 5))
  done
  
  log_error "Timeout waiting for cluster"
  docker compose -f "$COMPOSE_FILE" ps
  return 1
}

stop_cluster() {
  log_info "Stopping cluster..."
  cd "$SCRIPT_DIR"
  docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
  log_success "Cluster stopped"
}

# =============================================================================
# Temporal Graph Tests
# =============================================================================

run_temporal_test() {
  local node_count=$1
  local test_type=$2
  local duration=${3:-30}
  local timestamp=$(date +%Y%m%d_%H%M%S)
  local result_file="$RESULTS_DIR/temporal_${node_count}nodes_${test_type}_${timestamp}.md"
  
  log_header ""
  log_header "========================================"
  log_header "Temporal Graph Test: ${node_count} Nodes"
  log_header "Test Type: ${test_type}"
  log_header "========================================"
  
  # Build command based on test type
  local extra_args=""
  case $test_type in
    point)
      extra_args="--test-point --point-ratio 100"
      ;;
    range)
      extra_args="--test-range --range-ratio 100"
      ;;
    analytics)
      extra_args="--test-analytics --analytics-ratio 100"
      ;;
    write)
      extra_args="--test-write --write-ratio 100"
      ;;
    mixed)
      extra_args="--point-ratio 30 --range-ratio 30 --analytics-ratio 20 --write-ratio 20"
      ;;
  esac
  
  # Run benchmark in Docker container
  log_info "Running benchmark..."
  
  # Copy binary to container and execute
  docker cp "$BENCHMARK_BIN" cedar-storaged1:/tmp/test_temporal_graph_perf
  docker compose -f "$COMPOSE_FILE" exec -T storaged1 chmod +x /tmp/test_temporal_graph_perf
  
  # Execute test
  docker compose -f "$COMPOSE_FILE" exec -T storaged1 \
    /tmp/test_temporal_graph_perf \
    --nodes $node_count \
    --duration $duration \
    --clients $((node_count * 4)) \
    --vertices 100000 \
    --edges 500000 \
    $extra_args \
    --output /tmp/result.md 2>&1 | tee "$result_file"
  
  # Copy result back
  docker cp cedar-storaged1:/tmp/result.md "$result_file" 2>/dev/null || true
  
  log_success "Test completed: $test_type"
}

run_full_test_suite() {
  local node_count=$1
  local duration=${2:-30}
  
  log_header ""
  log_header "############################################################"
  log_header "#                                                          #"
  log_header "#   Temporal Graph Test Suite - ${node_count} Nodes                     #"
  log_header "#                                                          #"
  log_header "############################################################"
  log_header ""
  
  # Start cluster
  start_cluster $node_count
  sleep 5
  
  # Run all test types
  run_temporal_test $node_count "point" $duration
  run_temporal_test $node_count "range" $duration
  run_temporal_test $node_count "analytics" $duration
  run_temporal_test $node_count "write" $duration
  run_temporal_test $node_count "mixed" $duration
  
  # Stop cluster
  stop_cluster
}

# =============================================================================
# Report Generation
# =============================================================================

generate_comparison_report() {
  local report_file="$RESULTS_DIR/temporal_comparison_report_$(date +%Y%m%d_%H%M%S).md"
  
  log_info "Generating comparison report..."
  
  cat > "$report_file" << 'EOF'
# CedarGraph Temporal Graph Performance Report

## Test Overview

This report compares the performance of temporal graph operations across different cluster sizes.

## Test Scenarios

1. **Temporal Point Query**: Query vertex/edge state at a specific timestamp
2. **Temporal Range Query**: Query all changes within a time range
3. **Graph Analytics**: PageRank, Shortest Path, Connected Components, Temporal Pattern Matching
4. **Temporal Write**: Insert/update vertices and edges with timestamps
5. **Mixed Workload**: Realistic mix of all operation types

EOF

  # Find and summarize all results
  for node_count in 3 5 7; do
    echo "## ${node_count}-Node Cluster Results" >> "$report_file"
    echo "" >> "$report_file"
    
    for test_type in point range analytics write mixed; do
      local result_file=$(ls -t "$RESULTS_DIR"/temporal_${node_count}nodes_${test_type}_*.md 2>/dev/null | head -1)
      if [ -f "$result_file" ]; then
        echo "### ${test_type}" >> "$report_file"
        echo "" >> "$report_file"
        grep "Throughput" "$result_file" >> "$report_file" || true
        echo "" >> "$report_file"
      fi
    done
  done
  
  log_success "Report generated: $report_file"
}

# =============================================================================
# Main
# =============================================================================

main() {
  local node_count="${1:-3}"
  local test_mode="${2:-full}"
  
  case "$node_count" in
    3|5|7)
      check_prerequisites
      run_full_test_suite $node_count
      ;;
    all)
      check_prerequisites
      log_header ""
      log_header "############################################################"
      log_header "#                                                          #"
      log_header "#   Temporal Graph Scaling Test                            #"
      log_header "#   Testing 3, 5, and 7 node configurations               #"
      log_header "#                                                          #"
      log_header "############################################################"
      log_header ""
      
      run_full_test_suite 3
      run_full_test_suite 5
      run_full_test_suite 7
      
      generate_comparison_report
      ;;
    *)
      log_error "Invalid node count: $node_count"
      echo "Usage: $0 [3|5|7|all]"
      exit 1
      ;;
  esac
  
  log_header ""
  log_header "############################################################"
  log_header "#                                                          #"
  log_header "#   All tests completed!                                   #"
  log_header "#   Results saved to: $RESULTS_DIR"
  log_header "#                                                          #"
  log_header "############################################################"
  log_header ""
}

# Handle arguments
case "${1:-}" in
  --help|-h)
    echo "CedarGraph Temporal Graph Docker Test"
    echo ""
    echo "Usage: $0 [node_count]"
    echo ""
    echo "Node Count:"
    echo "  3          Test 3-node cluster"
    echo "  5          Test 5-node cluster"
    echo "  7          Test 7-node cluster"
    echo "  all        Test all configurations (3, 5, 7)"
    echo ""
    echo "Examples:"
    echo "  $0 3       # Test temporal queries on 3-node cluster"
    echo "  $0 all     # Full scaling test across 3/5/7 nodes"
    echo ""
    ;;
  *)
    main "$@"
    ;;
esac
