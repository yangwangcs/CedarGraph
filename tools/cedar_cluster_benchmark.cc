// tools/cedar_cluster_benchmark.cc
// CedarGraph 3-Node Cluster Temporal Graph Performance Benchmark Main Program

#include <iostream>
#include <vector>
#include <memory>
#include <chrono>
#include <filesystem>

#include "benchmark_metrics.h"
#include "benchmark_workloads.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"

using namespace cedar;
using namespace cedar::benchmark;

// 3-Node Cluster Configuration
struct ClusterConfig {
  std::vector<std::pair<std::string, int>> nodes = {
    {"node-0", 9779},
    {"node-1", 9780},
    {"node-2", 9781}
  };
  std::string data_dir = "/tmp/cedar_benchmark";
};

void PrintBanner() {
  std::cout << R"(
   ____          _              ____                 _   _
  / ___|__ _ ___| |_ ___ _ __  / ___|_ __ __ _ _ __ | |__  ___ _ __
 | |   / _` / __| __/ _ \ '__|| |  _| '__/ _` | '_ \| '_ \/ _ \ '__|
 | |__| (_| \__ \ ||  __/ |   | |_| | | | (_| | |_) | | | |  __/ |
  \____\__,_|___/\__\___|_|    \____|_|  \__,_| .__/|_| |_|\___|_|
                                              |_|
              3-Node Cluster Performance Benchmark
)" << std::endl;
}

void PrintUsage(const char* program) {
  std::cout << "Usage: " << program << " [options]\n\n";
  std::cout << "Options:\n";
  std::cout << "  --all              Run all benchmarks\n";
  std::cout << "  --basic            Basic read/write benchmark\n";
  std::cout << "  --temporal-point   Temporal point query benchmark\n";
  std::cout << "  --temporal-range   Temporal range query benchmark\n";
  std::cout << "  --temporal-analytics Temporal analytics benchmark\n";
  std::cout << "  --graph-analytics  Graph analytics benchmark\n";
  std::cout << "  --realtime         Real-time latency benchmark\n";
  std::cout << "  --disk-usage       Measure disk usage\n";
  std::cout << "  --output <dir>     Output directory for reports (default: /tmp/cedar_benchmark)\n";
  std::cout << "  --help             Show this help\n";
}

CedarGraphStorage* InitializeStorage(const std::string& data_dir) {
  std::cout << "📦 Initializing storage...\n";
  
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);
  
  CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = false;  // Run in local mode for performance testing
  
  CedarGraphStorage* storage = nullptr;
  auto status = CedarGraphStorage::Open(options, data_dir, &storage);
  if (!status.ok()) {
    std::cerr << "❌ Failed to open storage: " << status.ToString() << std::endl;
    return nullptr;
  }
  
  // TODO: Reimplement with braft/brpc

  std::cout << "✅ Storage initialized\n\n";
  return storage;
}

void RunAllBenchmarks(CedarGraphStorage* storage, const std::string& output_dir, const std::string& data_dir) {
  std::vector<std::unique_ptr<BenchmarkRunner>> benchmarks;
  
  // 1. Basic Read/Write
  benchmarks.push_back(std::make_unique<BasicReadWriteWorkload>(
    BasicReadWriteWorkload::Config{100000, 10000, 8, 20, 100},
    storage
  ));
  
  // 2. Temporal Point Query
  benchmarks.push_back(std::make_unique<TemporalPointQueryWorkload>(
    TemporalPointQueryWorkload::Config{50000, 8, 1000, 86400},
    storage
  ));
  
  // 3. Temporal Range Query
  benchmarks.push_back(std::make_unique<TemporalRangeQueryWorkload>(
    TemporalRangeQueryWorkload::Config{10000, 4, 3600, 86400, 604800},
    storage
  ));
  
  // 4. Temporal Analytics
  benchmarks.push_back(std::make_unique<TemporalAnalyticsWorkload>(
    TemporalAnalyticsWorkload::Config{100, 50, 200, 10000},
    storage
  ));
  
  // 5. Graph Analytics
  benchmarks.push_back(std::make_unique<GraphAnalyticsWorkload>(
    GraphAnalyticsWorkload::Config{50000, 100, 50, 3, 4},
    storage
  ));
  
  // 6. Real-time Latency
  benchmarks.push_back(std::make_unique<RealtimeLatencyWorkload>(
    RealtimeLatencyWorkload::Config{60, 1000, 10},
    storage
  ));
  
  std::cout << "🚀 Running " << benchmarks.size() << " benchmarks...\n\n";
  
  for (size_t i = 0; i < benchmarks.size(); i++) {
    std::cout << "[" << (i + 1) << "/" << benchmarks.size() << "] ";
    
    BenchmarkReport report;
    report.benchmark_name = benchmarks[i]->GetName();
    report.cluster_config = "3-node (9779,9780,9781)";
    report.start_time = std::chrono::system_clock::now();
    
    benchmarks[i]->Run(report);
    
    report.end_time = std::chrono::system_clock::now();
    
    // Measure disk usage
    report.disk_metrics.Measure(data_dir);
    
    // Print and save report
    report.PrintConsole();
    
    std::string json_file = output_dir + "/report_" + 
                           std::to_string(i) + "_" + 
                           benchmarks[i]->GetName() + ".json";
    // Replace spaces with underscores for filename
    std::replace(json_file.begin(), json_file.end(), ' ', '_');
    std::replace(json_file.begin(), json_file.end(), '/', '_');
    
    report.SaveToJson(json_file);
    std::cout << "   Report saved: " << json_file << "\n\n";
  }
}

void RunSingleBenchmark(const std::string& mode, CedarGraphStorage* storage, 
                        const std::string& output_dir) {
  std::unique_ptr<BenchmarkRunner> benchmark;
  
  if (mode == "--basic") {
    benchmark = std::make_unique<BasicReadWriteWorkload>(
      BasicReadWriteWorkload::Config{100000, 10000, 8, 20, 100},
      storage
    );
  } else if (mode == "--temporal-point") {
    benchmark = std::make_unique<TemporalPointQueryWorkload>(
      TemporalPointQueryWorkload::Config{50000, 8, 1000, 86400},
      storage
    );
  } else if (mode == "--temporal-range") {
    benchmark = std::make_unique<TemporalRangeQueryWorkload>(
      TemporalRangeQueryWorkload::Config{10000, 4, 3600, 86400, 604800},
      storage
    );
  } else if (mode == "--temporal-analytics") {
    benchmark = std::make_unique<TemporalAnalyticsWorkload>(
      TemporalAnalyticsWorkload::Config{100, 50, 200, 10000},
      storage
    );
  } else if (mode == "--graph-analytics") {
    benchmark = std::make_unique<GraphAnalyticsWorkload>(
      GraphAnalyticsWorkload::Config{50000, 100, 50, 3, 4},
      storage
    );
  } else if (mode == "--realtime") {
    benchmark = std::make_unique<RealtimeLatencyWorkload>(
      RealtimeLatencyWorkload::Config{60, 1000, 10},
      storage
    );
  } else {
    std::cerr << "❌ Unknown benchmark mode: " << mode << "\n";
    return;
  }
  
  BenchmarkReport report;
  report.benchmark_name = benchmark->GetName();
  report.cluster_config = "3-node (9779,9780,9781)";
  report.start_time = std::chrono::system_clock::now();
  
  benchmark->Run(report);
  
  report.end_time = std::chrono::system_clock::now();
  
  // Measure disk usage
  report.disk_metrics.Measure(output_dir + "/data");
  
  // Print and save report
  report.PrintConsole();
  
  std::string json_file = output_dir + "/report_single_" + mode.substr(2) + ".json";
  report.SaveToJson(json_file);
  std::cout << "   Report saved: " << json_file << "\n";
}

int main(int argc, char* argv[]) {
  PrintBanner();
  
  if (argc < 2) {
    PrintUsage(argv[0]);
    return 1;
  }
  
  std::string output_dir = "/tmp/cedar_benchmark";
  std::string mode = argv[1];
  
  // Parse arguments
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--output" && i + 1 < argc) {
      output_dir = argv[++i];
    } else if (arg == "--help") {
      PrintUsage(argv[0]);
      return 0;
    }
  }
  
  // Handle help
  if (mode == "--help") {
    PrintUsage(argv[0]);
    return 0;
  }
  
  std::cout << "📁 Output directory: " << output_dir << "\n";
  std::cout << "🖥️  Cluster: 3-node (127.0.0.1:9779, 9780, 9781)\n\n";
  
  // Create output directory
  std::filesystem::create_directories(output_dir);
  
  // Handle disk usage only mode
  if (mode == "--disk-usage") {
    std::cout << "📊 Measuring disk usage...\n";
    DiskUsageMetrics disk_metrics;
    disk_metrics.Measure(output_dir + "/data");
    disk_metrics.Print();
    return 0;
  }
  
  // Initialize storage
  CedarGraphStorage* storage = InitializeStorage(output_dir + "/data");
  if (!storage) {
    return 1;
  }
  
  // Run benchmarks
  if (mode == "--all") {
    RunAllBenchmarks(storage, output_dir, output_dir + "/data");
  } else {
    RunSingleBenchmark(mode, storage, output_dir);
  }
  
  // Cleanup
  std::cout << "🧹 Cleaning up...\n";
  delete storage;
  
  std::cout << "\n✨ All benchmarks completed!\n";
  return 0;
}
