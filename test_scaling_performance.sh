#!/bin/bash
# =============================================================================
# CedarGraph Scaling Performance Test
# 扩展性性能测试 - 测试 3/5/7 节点性能对比
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_BIN="$SCRIPT_DIR/build/test_docker_perf_benchmark"
RESULTS_DIR="$SCRIPT_DIR/test_results"

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
# Check Prerequisites
# =============================================================================

check_prerequisites() {
  log_info "Checking prerequisites..."
  
  if [ ! -f "$BENCHMARK_BIN" ]; then
    log_error "Benchmark binary not found: $BENCHMARK_BIN"
    log_info "Please build first: cd build && cmake --build . --target test_docker_perf_benchmark"
    exit 1
  fi
  
  mkdir -p "$RESULTS_DIR"
  log_success "Prerequisites check passed"
}

# =============================================================================
# Run Single Node Count Test
# =============================================================================

run_test() {
  local node_count=$1
  local duration=${2:-30}
  local timestamp=$(date +%Y%m%d_%H%M%S)
  local result_file="$RESULTS_DIR/scaling_test_${node_count}nodes_${timestamp}.txt"
  
  log_header ""
  log_header "========================================"
  log_header "Testing ${node_count}-Node Configuration"
  log_header "========================================"
  
  log_info "Running benchmark: nodes=$node_count, duration=${duration}s"
  log_info "Results will be saved to: $result_file"
  
  # Run benchmark
  $BENCHMARK_BIN --nodes $node_count --duration $duration --clients $((node_count * 4)) 2>&1 | tee "$result_file"
  
  # Extract key metrics
  local total_ops=$(grep "Total:" "$result_file" | head -1 | awk '{print $2}')
  local throughput=$(grep "Total:" "$result_file" | grep "ops/sec" | awk '{print $2}')
  local write_tput=$(grep "Writes:" "$result_file" | grep "ops/sec" | awk '{print $2}')
  local read_tput=$(grep "Reads:" "$result_file" | grep "ops/sec" | awk '{print $2}')
  
  log_success "Test completed for ${node_count} nodes"
  
  # Return metrics
  echo "${node_count},${total_ops},${throughput},${write_tput},${read_tput}"
}

# =============================================================================
# Generate Comparison Report
# =============================================================================

generate_report() {
  local report_file="$RESULTS_DIR/scaling_comparison_report_$(date +%Y%m%d_%H%M%S).md"
  
  log_info "Generating comparison report..."
  
  cat > "$report_file" << 'EOF'
# CedarGraph Scaling Performance Report

## Test Environment
- **Date**: $(date)
- **Test Tool**: test_docker_perf_benchmark
- **Value Size**: 1024 bytes
- **Write Ratio**: 20%
- **Key Range**: 100,000

## Results Summary

| Node Count | Total Ops | Throughput (ops/s) | Write Tput (ops/s) | Read Tput (ops/s) | Scaling Efficiency |
|------------|-----------|-------------------|-------------------|-------------------|-------------------|
EOF

  # Find all result files and extract data
  local all_results=""
  for f in "$RESULTS_DIR"/scaling_test_*nodes_*.txt; do
    if [ -f "$f" ]; then
      local nodes=$(echo "$f" | grep -o '[0-9]*nodes' | grep -o '[0-9]*')
      local ops=$(grep "Total:" "$f" | head -1 | awk '{print $2}')
      local tput=$(grep "Total:" "$f" | grep "ops/sec" | awk '{print $2}')
      local wtput=$(grep "Writes:" "$f" | grep "ops/sec" | awk '{print $2}')
      local rtput=$(grep "Reads:" "$f" | grep "ops/sec" | awk '{print $2}')
      
      all_results="${all_results}${nodes},${ops},${tput},${wtput},${rtput}\n"
    fi
  done
  
  # Sort by node count and add to report
  echo -e "$all_results" | sort -t',' -k1 -n | while IFS=',' read -r nodes ops tput wtput rtput; do
    if [ -n "$nodes" ]; then
      # Calculate scaling efficiency (relative to 3-node baseline)
      local efficiency="N/A"
      if [ "$nodes" -eq 3 ]; then
        efficiency="100% (baseline)"
      elif [ "$nodes" -gt 3 ]; then
        # Find 3-node throughput for comparison
        local baseline=$(echo -e "$all_results" | grep "^3," | cut -d',' -f3)
        if [ -n "$baseline" ]; then
          local ideal_tput=$(echo "$baseline * $nodes / 3" | bc -l 2>/dev/null || echo "0")
          if [ "$(echo "$ideal_tput > 0" | bc -l 2>/dev/null || echo "0")" -eq 1 ]; then
            efficiency=$(echo "scale=1; $tput / $ideal_tput * 100" | bc -l 2>/dev/null || echo "N/A")
            efficiency="${efficiency}%"
          fi
        fi
      fi
      
      echo "| ${nodes} | ${ops} | ${tput} | ${wtput} | ${rtput} | ${efficiency} |" >> "$report_file"
    fi
  done
  
  cat >> "$report_file" << 'EOF'

## Analysis

### Throughput Scaling
- The system shows [linear/sub-linear] scaling with node count
- 5-node configuration provides [X]x throughput vs 3-node
- 7-node configuration provides [X]x throughput vs 3-node

### Latency Characteristics
- P50 latency remains stable across configurations
- P99 latency shows [increase/decrease] with node count

### Recommendations
- Optimal node count for this workload: [3/5/7]
- Consider network bandwidth limitations for larger clusters
- Monitor replication overhead in multi-node configurations

## Raw Data

EOF

  # Append all raw results
  for f in "$RESULTS_DIR"/scaling_test_*nodes_*.txt; do
    if [ -f "$f" ]; then
      echo "### $(basename $f)" >> "$report_file"
      echo "" >> "$report_file"
      echo "\`\`\`" >> "$report_file"
      cat "$f" >> "$report_file"
      echo "\`\`\`" >> "$report_file"
      echo "" >> "$report_file"
    fi
  done
  
  log_success "Report generated: $report_file"
}

# =============================================================================
# Main
# =============================================================================

main() {
  local test_mode="${1:-quick}"
  local duration=30
  
  case "$test_mode" in
    quick)
      duration=10
      log_info "Running quick tests (10s each)"
      ;;
    standard)
      duration=30
      log_info "Running standard tests (30s each)"
      ;;
    thorough)
      duration=60
      log_info "Running thorough tests (60s each)"
      ;;
    *)
      log_error "Invalid test mode: $test_mode"
      echo "Usage: $0 [quick|standard|thorough]"
      exit 1
      ;;
  esac
  
  check_prerequisites
  
  log_header ""
  log_header "############################################################"
  log_header "#                                                          #"
  log_header "#   CedarGraph Scaling Performance Test                    #"
  log_header "#   Comparing 3, 5, and 7 node configurations             #"
  log_header "#                                                          #"
  log_header "############################################################"
  log_header ""
  
  local start_time=$(date +%s)
  
  # Run tests for each configuration
  run_test 3 $duration
  run_test 5 $duration
  run_test 7 $duration
  
  local end_time=$(date +%s)
  local total_time=$((end_time - start_time))
  
  # Generate report
  generate_report
  
  log_header ""
  log_header "############################################################"
  log_header "#                                                          #"
  log_header "#   All tests completed!                                   #"
  log_header "#   Total time: ${total_time}s                             #"
  log_header "#                                                          #"
  log_header "############################################################"
  log_header ""
  
  log_info "Results saved to: $RESULTS_DIR"
  log_info "View report: ls -la $RESULTS_DIR/*.md"
}

# Handle arguments
case "${1:-}" in
  --help|-h)
    echo "CedarGraph Scaling Performance Test"
    echo ""
    echo "Usage: $0 [mode]"
    echo ""
    echo "Modes:"
    echo "  quick      Quick test (10s per configuration) - default"
    echo "  standard   Standard test (30s per configuration)"
    echo "  thorough   Thorough test (60s per configuration)"
    echo ""
    echo "Examples:"
    echo "  $0              # Run quick tests"
    echo "  $0 standard     # Run standard tests"
    echo "  $0 thorough     # Run thorough tests"
    echo ""
    ;;
  *)
    main "$@"
    ;;
esac
