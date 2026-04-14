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
// CedarGraph Docker Performance Benchmark
// Docker 性能基准测试 - 测试 3/5/7 节点性能
// =============================================================================

#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <numeric>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

using namespace cedar;
using namespace cedar::dtx;

// =============================================================================
// Performance Test Configuration
// =============================================================================

struct BenchmarkConfig {
  int node_count = 3;                    // 节点数量
  int duration_seconds = 60;             // 测试持续时间
  int concurrent_clients = 16;           // 并发客户端数
  int write_ratio = 20;                  // 写操作比例 (%)
  int value_size = 1024;                 // 值大小 (bytes)
  int key_range = 100000;                // Key 范围
  std::vector<std::string> endpoints;    // 存储节点地址
  
  void Print() const {
    std::cout << "Benchmark Configuration:" << std::endl;
    std::cout << "  Node Count: " << node_count << std::endl;
    std::cout << "  Duration: " << duration_seconds << "s" << std::endl;
    std::cout << "  Concurrent Clients: " << concurrent_clients << std::endl;
    std::cout << "  Write Ratio: " << write_ratio << "%" << std::endl;
    std::cout << "  Value Size: " << value_size << " bytes" << std::endl;
    std::cout << "  Key Range: " << key_range << std::endl;
    std::cout << "  Endpoints: ";
    for (const auto& ep : endpoints) {
      std::cout << ep << " ";
    }
    std::cout << std::endl;
  }
};

// =============================================================================
// Performance Metrics
// =============================================================================

struct PerformanceMetrics {
  std::atomic<uint64_t> total_ops{0};
  std::atomic<uint64_t> write_ops{0};
  std::atomic<uint64_t> read_ops{0};
  std::atomic<uint64_t> failed_ops{0};
  
  // Latency histogram (microseconds)
  std::vector<uint64_t> write_latencies;
  std::vector<uint64_t> read_latencies;
  std::mutex latency_mutex;
  
  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point end_time;
  
  void Start() {
    start_time = std::chrono::steady_clock::now();
  }
  
  void Stop() {
    end_time = std::chrono::steady_clock::now();
  }
  
  void RecordWriteLatency(uint64_t latency_us) {
    std::lock_guard<std::mutex> lock(latency_mutex);
    write_latencies.push_back(latency_us);
  }
  
  void RecordReadLatency(uint64_t latency_us) {
    std::lock_guard<std::mutex> lock(latency_mutex);
    read_latencies.push_back(latency_us);
  }
  
  double GetDurationSeconds() const {
    auto duration = end_time - start_time;
    return std::chrono::duration<double>(duration).count();
  }
  
  double GetThroughput() const {
    return total_ops.load() / GetDurationSeconds();
  }
  
  double GetWriteThroughput() const {
    return write_ops.load() / GetDurationSeconds();
  }
  
  double GetReadThroughput() const {
    return read_ops.load() / GetDurationSeconds();
  }
  
  uint64_t GetP50Latency(const std::vector<uint64_t>& latencies) const {
    if (latencies.empty()) return 0;
    auto sorted = latencies;
    std::sort(sorted.begin(), sorted.end());
    return sorted[sorted.size() * 0.5];
  }
  
  uint64_t GetP99Latency(const std::vector<uint64_t>& latencies) const {
    if (latencies.empty()) return 0;
    auto sorted = latencies;
    std::sort(sorted.begin(), sorted.end());
    return sorted[sorted.size() * 0.99];
  }
  
  void PrintReport() const {
    auto duration = GetDurationSeconds();
    
    std::cout << "\n╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║           Performance Test Results                         ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    
    std::cout << "Duration: " << std::fixed << std::setprecision(2) << duration << " seconds" << std::endl;
    std::cout << std::endl;
    
    std::cout << "Operations:" << std::endl;
    std::cout << "  Total:  " << total_ops.load() << std::endl;
    std::cout << "  Writes: " << write_ops.load() << std::endl;
    std::cout << "  Reads:  " << read_ops.load() << std::endl;
    std::cout << "  Failed: " << failed_ops.load() << std::endl;
    std::cout << std::endl;
    
    std::cout << "Throughput:" << std::endl;
    std::cout << "  Total:  " << std::fixed << std::setprecision(1) << GetThroughput() << " ops/sec" << std::endl;
    std::cout << "  Writes: " << GetWriteThroughput() << " ops/sec" << std::endl;
    std::cout << "  Reads:  " << GetReadThroughput() << " ops/sec" << std::endl;
    std::cout << std::endl;
    
    if (!write_latencies.empty()) {
      std::cout << "Write Latency:" << std::endl;
      std::cout << "  P50: " << GetP50Latency(write_latencies) << " µs" << std::endl;
      std::cout << "  P99: " << GetP99Latency(write_latencies) << " µs" << std::endl;
    }
    
    if (!read_latencies.empty()) {
      std::cout << "Read Latency:" << std::endl;
      std::cout << "  P50: " << GetP50Latency(read_latencies) << " µs" << std::endl;
      std::cout << "  P99: " << GetP99Latency(read_latencies) << " µs" << std::endl;
    }
    
    std::cout << std::endl;
  }
};

// =============================================================================
// Simulated Storage Client for Benchmarking
// =============================================================================

class BenchmarkClient {
 public:
  BenchmarkClient(int client_id, const BenchmarkConfig& config, PerformanceMetrics& metrics)
      : client_id_(client_id), config_(config), metrics_(metrics), 
        rng_(std::random_device{}() + client_id), running_(false) {}
  
  void Start() {
    running_ = true;
    thread_ = std::thread([this]() { Run(); });
  }
  
  void Stop() {
    running_ = false;
    if (thread_.joinable()) {
      thread_.join();
    }
  }
  
 private:
  void Run() {
    std::uniform_int_distribution<int> op_dist(0, 99);
    std::uniform_int_distribution<uint64_t> key_dist(0, config_.key_range - 1);
    
    while (running_) {
      auto start = std::chrono::high_resolution_clock::now();
      
      bool is_write = op_dist(rng_) < config_.write_ratio;
      uint64_t key = key_dist(rng_);
      
      Status status;
      if (is_write) {
        status = SimulateWrite(key);
      } else {
        status = SimulateRead(key);
      }
      
      auto end = std::chrono::high_resolution_clock::now();
      auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      
      if (status.ok()) {
        metrics_.total_ops.fetch_add(1);
        if (is_write) {
          metrics_.write_ops.fetch_add(1);
          metrics_.RecordWriteLatency(latency_us);
        } else {
          metrics_.read_ops.fetch_add(1);
          metrics_.RecordReadLatency(latency_us);
        }
      } else {
        metrics_.failed_ops.fetch_add(1);
      }
      
      // Small delay to prevent overwhelming the system
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }
  
  Status SimulateWrite(uint64_t key) {
    // Simulate write operation
    // In real test, this would call actual storage client
    std::uniform_int_distribution<int> fail_dist(0, 99);
    if (fail_dist(rng_) < 1) {  // 1% failure rate
      return Status::IOError("Simulated write failure");
    }
    return Status::OK();
  }
  
  Status SimulateRead(uint64_t key) {
    // Simulate read operation
    std::uniform_int_distribution<int> fail_dist(0, 99);
    if (fail_dist(rng_) < 0) {  // 0% failure rate for reads
      return Status::NotFound("Key not found");
    }
    return Status::OK();
  }
  
  int client_id_;
  const BenchmarkConfig& config_;
  PerformanceMetrics& metrics_;
  std::mt19937 rng_;
  std::atomic<bool> running_;
  std::thread thread_;
};

// =============================================================================
// Benchmark Runner
// =============================================================================

class BenchmarkRunner {
 public:
  BenchmarkRunner(const BenchmarkConfig& config) : config_(config) {}
  
  void Run() {
    std::cout << "\n╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║     CedarGraph Docker Performance Benchmark                ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    
    config_.Print();
    std::cout << std::endl;
    
    // Create clients
    std::vector<std::unique_ptr<BenchmarkClient>> clients;
    for (int i = 0; i < config_.concurrent_clients; ++i) {
      clients.push_back(std::make_unique<BenchmarkClient>(i, config_, metrics_));
    }
    
    // Start benchmark
    std::cout << "Starting benchmark..." << std::endl;
    metrics_.Start();
    
    for (auto& client : clients) {
      client->Start();
    }
    
    // Progress reporting
    std::thread progress_thread([this]() {
      auto start = std::chrono::steady_clock::now();
      while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start).count();
        
        if (elapsed >= config_.duration_seconds) break;
        
        auto ops = metrics_.total_ops.load();
        auto throughput = ops / std::max(1.0, (double)elapsed);
        
        std::cout << "[Progress] " << elapsed << "s / " << config_.duration_seconds 
                  << "s | Ops: " << ops << " | Throughput: " << std::fixed 
                  << std::setprecision(1) << throughput << " ops/sec" << std::endl;
      }
    });
    
    // Wait for duration
    std::this_thread::sleep_for(std::chrono::seconds(config_.duration_seconds));
    
    // Stop clients
    for (auto& client : clients) {
      client->Stop();
    }
    
    metrics_.Stop();
    progress_thread.join();
    
    // Print results
    metrics_.PrintReport();
  }
  
  const PerformanceMetrics& GetMetrics() const {
    return metrics_;
  }
  
 private:
  BenchmarkConfig config_;
  PerformanceMetrics metrics_;
};

// =============================================================================
// Main
// =============================================================================

int main(int argc, char* argv[]) {
  BenchmarkConfig config;
  
  // Parse arguments
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--nodes" && i + 1 < argc) {
      config.node_count = std::stoi(argv[++i]);
    } else if (arg == "--duration" && i + 1 < argc) {
      config.duration_seconds = std::stoi(argv[++i]);
    } else if (arg == "--clients" && i + 1 < argc) {
      config.concurrent_clients = std::stoi(argv[++i]);
    } else if (arg == "--write-ratio" && i + 1 < argc) {
      config.write_ratio = std::stoi(argv[++i]);
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "CedarGraph Docker Performance Benchmark\n"
                << "Usage: " << argv[0] << " [options]\n"
                << "\nOptions:\n"
                << "  --nodes <n>         Number of nodes (3/5/7, default: 3)\n"
                << "  --duration <sec>    Test duration in seconds (default: 60)\n"
                << "  --clients <n>       Number of concurrent clients (default: 16)\n"
                << "  --write-ratio <%>   Write operation ratio (default: 20)\n"
                << "  --help              Show this help\n";
      return 0;
    }
  }
  
  // Set endpoints based on node count
  for (int i = 1; i <= config.node_count; ++i) {
    std::stringstream ss;
    ss << "storaged" << i << ":700" << (i - 1);
    config.endpoints.push_back(ss.str());
  }
  
  // Run benchmark
  BenchmarkRunner runner(config);
  runner.Run();
  
  return 0;
}
