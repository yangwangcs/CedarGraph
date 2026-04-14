#!/bin/bash
# =============================================================================
# CedarGraph Temporal Graph Scenario Tests
# 时态图场景测试套件
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_BIN="$SCRIPT_DIR/build/test_temporal_graph_perf"
RESULTS_DIR="$SCRIPT_DIR/test_results/temporal_scenarios"

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

mkdir -p "$RESULTS_DIR"

# =============================================================================
# Test Scenarios
# =============================================================================

scenario_social_network() {
  log_header "========================================"
  log_header "Scenario: Social Network Analysis"
  log_header "========================================"
  log_info "High point query ratio (user profile lookups)"
  log_info "Graph: 100K vertices, 500K edges"
  
  local output="$RESULTS_DIR/social_network_3nodes.md"
  $BENCHMARK_BIN --nodes 3 --duration 30 --vertices 100000 --edges 500000 \
    --point-ratio 70 --range-ratio 20 --analytics-ratio 10 --output "$output"
  
  log_success "Social network scenario completed"
}

scenario_financial_trading() {
  log_header "========================================"
  log_header "Scenario: Financial Trading Graph"
  log_header "========================================"
  log_info "High write ratio (transaction recording)"
  log_info "High range query (time-window analysis)"
  
  local output="$RESULTS_DIR/financial_trading_3nodes.md"
  $BENCHMARK_BIN --nodes 3 --duration 30 --vertices 50000 --edges 200000 \
    --point-ratio 20 --range-ratio 40 --write-ratio 40 --output "$output"
  
  log_success "Financial trading scenario completed"
}

scenario_knowledge_graph() {
  log_header "========================================"
  log_header "Scenario: Knowledge Graph Analytics"
  log_header "========================================"
  log_info "High analytics ratio (reasoning queries)"
  log_info "Large graph: 200K vertices, 1M edges"
  
  local output="$RESULTS_DIR/knowledge_graph_3nodes.md"
  $BENCHMARK_BIN --nodes 3 --duration 60 --vertices 200000 --edges 1000000 \
    --point-ratio 30 --analytics-ratio 70 --output "$output"
  
  log_success "Knowledge graph scenario completed"
}

scenario_iot_sensor_network() {
  log_header "========================================"
  log_header "Scenario: IoT Sensor Network"
  log_header "========================================"
  log_info "High write ratio (sensor data ingestion)"
  log_info "Time-series pattern: 50K vertices, 200K edges"
  
  local output="$RESULTS_DIR/iot_sensor_3nodes.md"
  $BENCHMARK_BIN --nodes 3 --duration 30 --vertices 50000 --edges 200000 \
    --point-ratio 10 --range-ratio 30 --write-ratio 60 --output "$output"
  
  log_success "IoT sensor scenario completed"
}

scenario_supply_chain() {
  log_header "========================================"
  log_header "Scenario: Supply Chain Tracking"
  log_header "========================================"
  log_info "Balanced workload with complex analytics"
  
  local output="$RESULTS_DIR/supply_chain_3nodes.md"
  $BENCHMARK_BIN --nodes 3 --duration 45 --vertices 80000 --edges 400000 \
    --point-ratio 25 --range-ratio 25 --analytics-ratio 25 --write-ratio 25 \
    --output "$output"
  
  log_success "Supply chain scenario completed"
}

scenario_temporal_pattern_mining() {
  log_header "========================================"
  log_header "Scenario: Temporal Pattern Mining"
  log_header "========================================"
  log_info "Focus on temporal range queries and patterns"
  
  local output="$RESULTS_DIR/pattern_mining_3nodes.md"
  $BENCHMARK_BIN --nodes 3 --duration 60 --vertices 150000 --edges 750000 \
    --point-ratio 15 --range-ratio 55 --analytics-ratio 30 --output "$output"
  
  log_success "Pattern mining scenario completed"
}

# =============================================================================
# Scaling Tests
# =============================================================================

scaling_test_read_heavy() {
  log_header "========================================"
  log_header "Scaling Test: Read-Heavy Workload"
  log_header "========================================"
  log_info "Testing 3, 5, 7 nodes with 90% read ratio"
  
  for nodes in 3 5 7; do
    log_info "Testing ${nodes}-node cluster..."
    local output="$RESULTS_DIR/scaling_read_heavy_${nodes}nodes.md"
    $BENCHMARK_BIN --nodes $nodes --duration 30 \
      --point-ratio 45 --range-ratio 45 --write-ratio 10 --output "$output"
  done
  
  log_success "Read-heavy scaling test completed"
}

scaling_test_write_heavy() {
  log_header "========================================"
  log_header "Scaling Test: Write-Heavy Workload"
  log_header "========================================"
  log_info "Testing 3, 5, 7 nodes with 60% write ratio"
  
  for nodes in 3 5 7; do
    log_info "Testing ${nodes}-node cluster..."
    local output="$RESULTS_DIR/scaling_write_heavy_${nodes}nodes.md"
    $BENCHMARK_BIN --nodes $nodes --duration 30 \
      --point-ratio 20 --range-ratio 20 --write-ratio 60 --output "$output"
  done
  
  log_success "Write-heavy scaling test completed"
}

# =============================================================================
# Stress Tests
# =============================================================================

stress_test_high_concurrency() {
  log_header "========================================"
  log_header "Stress Test: High Concurrency"
  log_header "========================================"
  log_info "Testing with 64 concurrent clients"
  
  local output="$RESULTS_DIR/stress_concurrency_3nodes.md"
  $BENCHMARK_BIN --nodes 3 --duration 30 --clients 64 \
    --vertices 100000 --edges 500000 --output "$output"
  
  log_success "High concurrency stress test completed"
}

stress_test_large_graph() {
  log_header "========================================"
  log_header "Stress Test: Large Graph"
  log_header "========================================"
  log_info "Testing with 500K vertices, 2.5M edges"
  
  local output="$RESULTS_DIR/stress_large_graph_3nodes.md"
  $BENCHMARK_BIN --nodes 3 --duration 60 --clients 16 \
    --vertices 500000 --edges 2500000 --output "$output"
  
  log_success "Large graph stress test completed"
}

stress_test_long_duration() {
  log_header "========================================"
  log_header "Stress Test: Long Duration"
  log_header "========================================"
  log_info "Testing for 5 minutes sustained load"
  
  local output="$RESULTS_DIR/stress_long_duration_3nodes.md"
  $BENCHMARK_BIN --nodes 3 --duration 300 --clients 16 \
    --vertices 100000 --edges 500000 --output "$output"
  
  log_success "Long duration stress test completed"
}

# =============================================================================
# Main
# =============================================================================

run_all_scenarios() {
  log_header ""
  log_header "############################################################"
  log_header "#                                                          #"
  log_header "#   CedarGraph Temporal Graph Scenario Tests               #"
  log_header "#                                                          #"
  log_header "############################################################"
  log_header ""
  
  local start_time=$(date +%s)
  
  # Run all scenarios
  scenario_social_network
  scenario_financial_trading
  scenario_knowledge_graph
  scenario_iot_sensor_network
  scenario_supply_chain
  scenario_temporal_pattern_mining
  
  # Run scaling tests
  scaling_test_read_heavy
  scaling_test_write_heavy
  
  # Run stress tests
  stress_test_high_concurrency
  stress_test_large_graph
  # stress_test_long_duration  # Uncomment for thorough testing
  
  local end_time=$(date +%s)
  local total_time=$((end_time - start_time))
  
  log_header ""
  log_header "############################################################"
  log_header "#                                                          #"
  log_header "#   All scenario tests completed!                          #"
  log_header "#   Total time: ${total_time}s                             #"
  log_header "#                                                          #"
  log_header "############################################################"
  log_header ""
  
  log_info "Results saved to: $RESULTS_DIR"
}

show_usage() {
  echo "CedarGraph Temporal Graph Scenario Tests"
  echo ""
  echo "Usage: $0 [scenario|all]"
  echo ""
  echo "Scenarios:"
  echo "  social           Social Network Analysis"
  echo "  financial        Financial Trading Graph"
  echo "  knowledge        Knowledge Graph Analytics"
  echo "  iot              IoT Sensor Network"
  echo "  supply           Supply Chain Tracking"
  echo "  pattern          Temporal Pattern Mining"
  echo "  scaling-read     Scaling test (read-heavy)"
  echo "  scaling-write    Scaling test (write-heavy)"
  echo "  stress-conc      Stress test (high concurrency)"
  echo "  stress-large     Stress test (large graph)"
  echo "  all              Run all scenarios (default)"
  echo ""
  echo "Examples:"
  echo "  $0 social        # Run social network scenario"
  echo "  $0 scaling-read  # Run read-heavy scaling test"
  echo "  $0 all           # Run all scenarios"
  echo ""
}

# Check prerequisites
if [ ! -f "$BENCHMARK_BIN" ]; then
  log_error "Benchmark binary not found: $BENCHMARK_BIN"
  log_info "Please build first: cd build && cmake --build . --target test_temporal_graph_perf"
  exit 1
fi

# Handle arguments
case "${1:-all}" in
  social) scenario_social_network ;;
  financial) scenario_financial_trading ;;
  knowledge) scenario_knowledge_graph ;;
  iot) scenario_iot_sensor_network ;;
  supply) scenario_supply_chain ;;
  pattern) scenario_temporal_pattern_mining ;;
  scaling-read) scaling_test_read_heavy ;;
  scaling-write) scaling_test_write_heavy ;;
  stress-conc) stress_test_high_concurrency ;;
  stress-large) stress_test_large_graph ;;
  all) run_all_scenarios ;;
  --help|-h) show_usage ;;
  *)
    log_error "Unknown scenario: $1"
    show_usage
    exit 1
    ;;
esac
