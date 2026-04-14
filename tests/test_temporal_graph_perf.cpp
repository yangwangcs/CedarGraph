// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// =============================================================================
// Temporal Graph Performance Test - Docker Multi-Node
// 时态图性能测试 - Docker 多节点环境
// =============================================================================

#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <cstring>

#include "cedar/dtx/temporal_graph_benchmark.h"

using namespace cedar::dtx;

// =============================================================================
// Command Line Arguments
// =============================================================================

struct TestConfig {
  int node_count = 3;
  int duration_seconds = 30;
  int concurrent_clients = 16;
  uint64_t vertex_count = 100000;
  uint64_t edge_count = 500000;
  
  // 查询比例
  int point_query_ratio = 30;
  int range_query_ratio = 30;
  int analytics_ratio = 20;
  int write_ratio = 20;
  
  // 测试类型
  bool test_point_query = true;
  bool test_range_query = true;
  bool test_analytics = true;
  bool test_write = true;
  
  // 输出
  std::string output_file;
};

void PrintUsage(const char* program) {
  std::cout << "CedarGraph Temporal Graph Performance Test\n"
            << "Usage: " << program << " [options]\n"
            << "\nOptions:\n"
            << "  --nodes <n>              Number of nodes (3/5/7, default: 3)\n"
            << "  --duration <sec>         Test duration in seconds (default: 30)\n"
            << "  --clients <n>            Concurrent clients (default: 16)\n"
            << "  --vertices <n>           Number of vertices (default: 100000)\n"
            << "  --edges <n>              Number of edges (default: 500000)\n"
            << "  --point-ratio <%>        Point query ratio (default: 30)\n"
            << "  --range-ratio <%>        Range query ratio (default: 30)\n"
            << "  --analytics-ratio <%>    Analytics ratio (default: 20)\n"
            << "  --write-ratio <%>        Write ratio (default: 20)\n"
            << "  --test-point             Run point query test only\n"
            << "  --test-range             Run range query test only\n"
            << "  --test-analytics         Run analytics test only\n"
            << "  --test-write             Run write test only\n"
            << "  --output <file>          Output results to file\n"
            << "  --help                   Show this help\n"
            << "\nExamples:\n"
            << "  # Test all query types on 5-node cluster\n"
            << "  " << program << " --nodes 5 --duration 60\n"
            << "\n  # Test only point queries\n"
            << "  " << program << " --test-point --point-ratio 100\n"
            << "\n  # Test with custom graph size\n"
            << "  " << program << " --nodes 7 --vertices 500000 --edges 2000000\n";
}

TestConfig ParseArgs(int argc, char* argv[]) {
  TestConfig config;
  
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    
    if (arg == "--nodes" && i + 1 < argc) {
      config.node_count = std::stoi(argv[++i]);
    } else if (arg == "--duration" && i + 1 < argc) {
      config.duration_seconds = std::stoi(argv[++i]);
    } else if (arg == "--clients" && i + 1 < argc) {
      config.concurrent_clients = std::stoi(argv[++i]);
    } else if (arg == "--vertices" && i + 1 < argc) {
      config.vertex_count = std::stoull(argv[++i]);
    } else if (arg == "--edges" && i + 1 < argc) {
      config.edge_count = std::stoull(argv[++i]);
    } else if (arg == "--point-ratio" && i + 1 < argc) {
      config.point_query_ratio = std::stoi(argv[++i]);
    } else if (arg == "--range-ratio" && i + 1 < argc) {
      config.range_query_ratio = std::stoi(argv[++i]);
    } else if (arg == "--analytics-ratio" && i + 1 < argc) {
      config.analytics_ratio = std::stoi(argv[++i]);
    } else if (arg == "--write-ratio" && i + 1 < argc) {
      config.write_ratio = std::stoi(argv[++i]);
    } else if (arg == "--test-point") {
      config.test_point_query = true;
      config.test_range_query = false;
      config.test_analytics = false;
      config.test_write = false;
    } else if (arg == "--test-range") {
      config.test_point_query = false;
      config.test_range_query = true;
      config.test_analytics = false;
      config.test_write = false;
    } else if (arg == "--test-analytics") {
      config.test_point_query = false;
      config.test_range_query = false;
      config.test_analytics = true;
      config.test_write = false;
    } else if (arg == "--test-write") {
      config.test_point_query = false;
      config.test_range_query = false;
      config.test_analytics = false;
      config.test_write = true;
    } else if (arg == "--output" && i + 1 < argc) {
      config.output_file = argv[++i];
    } else if (arg == "--help" || arg == "-h") {
      PrintUsage(argv[0]);
      exit(0);
    }
  }
  
  return config;
}

// =============================================================================
// Result Export
// =============================================================================

void ExportResults(const std::string& filename, int node_count, 
                   const TemporalQueryMetrics& metrics,
                   const TestConfig& config) {
  std::ofstream file(filename);
  if (!file.is_open()) {
    std::cerr << "Failed to open output file: " << filename << std::endl;
    return;
  }
  
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  
  file << "# CedarGraph Temporal Graph Performance Test Results\n\n";
  file << "**Test Time**: " << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") << "\n\n";
  
  file << "## Configuration\n\n";
  file << "| Parameter | Value |\n";
  file << "|-----------|-------|\n";
  file << "| Node Count | " << node_count << " |\n";
  file << "| Duration | " << config.duration_seconds << "s |\n";
  file << "| Clients | " << config.concurrent_clients << " |\n";
  file << "| Vertices | " << config.vertex_count << " |\n";
  file << "| Edges | " << config.edge_count << " |\n";
  file << "\n";
  
  file << "## Query Mix\n\n";
  file << "| Query Type | Ratio |\n";
  file << "|------------|-------|\n";
  file << "| Point Query | " << config.point_query_ratio << "% |\n";
  file << "| Range Query | " << config.range_query_ratio << "% |\n";
  file << "| Analytics | " << config.analytics_ratio << "% |\n";
  file << "| Write | " << config.write_ratio << "% |\n";
  file << "\n";
  
  double duration = metrics.GetDurationSeconds();
  file << "## Results\n\n";
  file << "| Metric | Value |\n";
  file << "|--------|-------|\n";
  file << "| Total Queries | " << metrics.total_queries.load() << " |\n";
  file << "| Point Queries | " << metrics.point_queries.load() << " |\n";
  file << "| Range Queries | " << metrics.range_queries.load() << " |\n";
  file << "| Analytics | " << metrics.analytics_queries.load() << " |\n";
  file << "| Write Ops | " << metrics.write_ops.load() << " |\n";
  file << "| Failed | " << metrics.failed_queries.load() << " |\n";
  file << "| Total Throughput | " << std::fixed << std::setprecision(2) 
       << (metrics.total_queries.load() / duration) << " qps |\n";
  file << "| Point Query Throughput | " << (metrics.point_queries.load() / duration) << " qps |\n";
  file << "| Range Query Throughput | " << (metrics.range_queries.load() / duration) << " qps |\n";
  file << "| Analytics Throughput | " << (metrics.analytics_queries.load() / duration) << " qps |\n";
  file << "| Write Throughput | " << (metrics.write_ops.load() / duration) << " ops/s |\n";
  
  file.close();
  std::cout << "\nResults exported to: " << filename << std::endl;
}

// =============================================================================
// Main
// =============================================================================

int main(int argc, char* argv[]) {
  TestConfig test_config = ParseArgs(argc, argv);
  
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     CedarGraph Temporal Graph Performance Test             ║" << std::endl;
  std::cout << "║     Docker Multi-Node Environment                          ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  // Validate node count
  if (test_config.node_count != 3 && test_config.node_count != 5 && test_config.node_count != 7) {
    std::cerr << "Error: Node count must be 3, 5, or 7" << std::endl;
    return 1;
  }
  
  // Build configuration
  TemporalBenchmarkConfig benchmark_config;
  benchmark_config.node_count = test_config.node_count;
  benchmark_config.duration_seconds = test_config.duration_seconds;
  benchmark_config.concurrent_clients = test_config.concurrent_clients;
  benchmark_config.vertex_count = test_config.vertex_count;
  benchmark_config.edge_count = test_config.edge_count;
  benchmark_config.point_query_ratio = test_config.point_query_ratio;
  benchmark_config.range_query_ratio = test_config.range_query_ratio;
  benchmark_config.graph_analytics_ratio = test_config.analytics_ratio;
  benchmark_config.write_ratio = test_config.write_ratio;
  
  // Build endpoints
  for (int i = 1; i <= test_config.node_count; ++i) {
    std::stringstream ss;
    ss << "storaged" << i << ":700" << (i - 1);
    benchmark_config.endpoints.push_back(ss.str());
  }
  
  // Run benchmark
  TemporalGraphBenchmark benchmark(benchmark_config);
  
  auto init_status = benchmark.Initialize();
  if (!init_status.ok()) {
    std::cerr << "Failed to initialize benchmark: " << init_status.ToString() << std::endl;
    return 1;
  }
  
  auto run_status = benchmark.Run();
  if (!run_status.ok()) {
    std::cerr << "Benchmark failed: " << run_status.ToString() << std::endl;
    return 1;
  }
  
  // Export results if requested
  if (!test_config.output_file.empty()) {
    ExportResults(test_config.output_file, test_config.node_count, 
                  benchmark.GetMetrics(), test_config);
  }
  
  return 0;
}
