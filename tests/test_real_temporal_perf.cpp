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
// REAL Temporal Graph Performance Test - Using Actual CedarGraph System
// 真实时态图性能测试 - 使用实际的 CedarGraph 系统
// =============================================================================

#include <iostream>
#include <chrono>
#include <iomanip>
#include <cstring>

#include "cedar/dtx/real_temporal_benchmark.h"

using namespace cedar::dtx;

void PrintBanner() {
  std::cout << "\n╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║                                                            ║" << std::endl;
  std::cout << "║     CedarGraph REAL Temporal Graph Performance Test        ║" << std::endl;
  std::cout << "║                                                            ║" << std::endl;
  std::cout << "║     Using Actual CedarGraphStorage + gRPC Clients          ║" << std::endl;
  std::cout << "║                                                            ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
}

void PrintUsage(const char* program) {
  std::cout << "Usage: " << program << " [options]\n"
            << "\nOptions:\n"
            << "  --nodes <n>              Number of nodes (3/5/7, default: 3)\n"
            << "  --duration <sec>         Test duration (default: 60)\n"
            << "  --clients <n>            Concurrent clients (default: 16)\n"
            << "  --vertices <n>           Number of vertices (default: 100000)\n"
            << "  --edges <n>              Number of edges (default: 500000)\n"
            << "  --data-dir <path>        Data directory (default: /tmp/cedar_benchmark)\n"
            << "  --point-ratio <%>        Point query ratio (default: 30)\n"
            << "  --range-ratio <%>        Range query ratio (default: 30)\n"
            << "  --analytics-ratio <%>    Analytics ratio (default: 20)\n"
            << "  --write-ratio <%>        Write ratio (default: 20)\n"
            << "  --batch-size <n>         Write batch size (default: 100)\n"
            << "  --no-batch               Disable batch writes\n"
            << "  --endpoints <list>       Comma-separated gRPC endpoints\n"
            << "  --help                   Show this help\n"
            << "\nExamples:\n"
            << "  # Test with default settings\n"
            << "  " << program << "\n"
            << "\n  # Test 5-node cluster with custom endpoints\n"
            << "  " << program << " --nodes 5 --endpoints \"localhost:7000,localhost:7001,localhost:7002\"\n"
            << "\n  # High write workload test\n"
            << "  " << program << " --write-ratio 80 --batch-size 200\n"
            << "\n  # Short test with small dataset\n"
            << "  " << program << " --duration 30 --vertices 10000 --edges 50000\n";
}

RealBenchmarkConfig ParseArgs(int argc, char* argv[]) {
  RealBenchmarkConfig config;
  
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
    } else if (arg == "--data-dir" && i + 1 < argc) {
      config.data_dir = argv[++i];
    } else if (arg == "--point-ratio" && i + 1 < argc) {
      config.point_query_ratio = std::stoi(argv[++i]);
    } else if (arg == "--range-ratio" && i + 1 < argc) {
      config.range_query_ratio = std::stoi(argv[++i]);
    } else if (arg == "--analytics-ratio" && i + 1 < argc) {
      config.graph_analytics_ratio = std::stoi(argv[++i]);
    } else if (arg == "--write-ratio" && i + 1 < argc) {
      config.write_ratio = std::stoi(argv[++i]);
    } else if (arg == "--batch-size" && i + 1 < argc) {
      config.batch_size = std::stoull(argv[++i]);
    } else if (arg == "--no-batch") {
      config.enable_batch_write = false;
    } else if (arg == "--endpoints" && i + 1 < argc) {
      std::string endpoints_str = argv[++i];
      config.endpoints.clear();
      
      // Parse comma-separated endpoints
      size_t start = 0;
      size_t end = endpoints_str.find(',');
      while (end != std::string::npos) {
        config.endpoints.push_back(endpoints_str.substr(start, end - start));
        start = end + 1;
        end = endpoints_str.find(',', start);
      }
      config.endpoints.push_back(endpoints_str.substr(start));
    } else if (arg == "--help" || arg == "-h") {
      PrintUsage(argv[0]);
      exit(0);
    }
  }
  
  // Validate
  if (config.node_count != 3 && config.node_count != 5 && config.node_count != 7) {
    std::cerr << "Warning: Node count should be 3, 5, or 7. Using 3." << std::endl;
    config.node_count = 3;
  }
  
  return config;
}

int main(int argc, char* argv[]) {
  PrintBanner();
  
  RealBenchmarkConfig config = ParseArgs(argc, argv);
  
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Node Count:        " << config.node_count << std::endl;
  std::cout << "  Duration:          " << config.duration_seconds << " seconds" << std::endl;
  std::cout << "  Concurrent Clients:" << config.concurrent_clients << std::endl;
  std::cout << "  Vertices:          " << config.vertex_count << std::endl;
  std::cout << "  Edges:             " << config.edge_count << std::endl;
  std::cout << "  Data Directory:    " << config.data_dir << std::endl;
  std::cout << "  Batch Writes:      " << (config.enable_batch_write ? "Enabled" : "Disabled") << std::endl;
  if (config.enable_batch_write) {
    std::cout << "  Batch Size:        " << config.batch_size << std::endl;
  }
  std::cout << "  Query Mix:         Point=" << config.point_query_ratio 
            << "%, Range=" << config.range_query_ratio
            << "%, Analytics=" << config.graph_analytics_ratio
            << "%, Write=" << config.write_ratio << "%" << std::endl;
  if (!config.endpoints.empty()) {
    std::cout << "  Endpoints:         ";
    for (const auto& ep : config.endpoints) {
      std::cout << ep << " ";
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;
  
  // Create and run benchmark
  RealTemporalBenchmark benchmark(config);
  
  // Initialize
  auto init_status = benchmark.Initialize();
  if (!init_status.ok()) {
    std::cerr << "Failed to initialize benchmark: " << init_status.ToString() << std::endl;
    return 1;
  }
  
  // Prepare data
  auto prepare_status = benchmark.PrepareData();
  if (!prepare_status.ok()) {
    std::cerr << "Failed to prepare data: " << prepare_status.ToString() << std::endl;
    return 1;
  }
  
  // Run benchmark
  auto run_status = benchmark.Run();
  if (!run_status.ok()) {
    std::cerr << "Benchmark failed: " << run_status.ToString() << std::endl;
    return 1;
  }
  
  // Cleanup
  benchmark.Cleanup();
  
  std::cout << "\nBenchmark completed successfully!" << std::endl;
  
  return 0;
}
