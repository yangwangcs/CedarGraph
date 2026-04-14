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
// Temporal Graph Performance Benchmark
// =============================================================================

#include <iostream>
#include <chrono>
#include <vector>
#include <thread>
#include <random>
#include <atomic>
#include <iomanip>
#include <cstring>
#include <filesystem>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/descriptor.h"
#include "cedar/core/status.h"
#include "cedar/db/graph_db.h"

using namespace cedar;

// =============================================================================
// Benchmark Configuration
// =============================================================================

struct BenchmarkConfig {
  // Data scale
  uint64_t num_vertices = 100000;
  uint64_t num_edges = 500000;
  uint64_t num_timestamps = 100;
  
  // Operations
  uint64_t write_iterations = 10000;
  uint64_t read_iterations = 100000;
  
  // Concurrency
  uint32_t num_threads = 4;
  
  // Data directory
  std::string data_dir = "/tmp/cedar/benchmark";
};

// =============================================================================
// Statistics
// =============================================================================

struct BenchmarkStats {
  std::atomic<uint64_t> write_count{0};
  std::atomic<uint64_t> read_count{0};
  
  std::atomic<uint64_t> write_latency_us{0};
  std::atomic<uint64_t> read_latency_us{0};
  
  std::atomic<uint64_t> bytes_written{0};
  std::atomic<uint64_t> bytes_read{0};
  
  void Print() const {
    auto write_avg = write_count.load() > 0 ? 
        write_latency_us.load() / write_count.load() : 0;
    auto read_avg = read_count.load() > 0 ? 
        read_latency_us.load() / read_count.load() : 0;
    
    std::cout << "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    std::cout << "  Performance Results\n";
    std::cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    std::cout << "  Write Operations:\n";
    std::cout << "    Count:      " << std::setw(10) << write_count.load() << " ops\n";
    std::cout << "    Avg Lat:    " << std::setw(10) << write_avg << " μs\n";
    std::cout << "    Throughput: " << std::setw(8) << std::fixed << std::setprecision(1) 
              << (write_count.load() * 1000000.0 / std::max(write_latency_us.load(), static_cast<uint64_t>(1))) << " ops/sec\n";
    std::cout << "    Data:       " << std::setw(10) << (bytes_written.load() / 1024 / 1024) << " MB\n";
    
    std::cout << "\n  Point Read Operations:\n";
    std::cout << "    Count:      " << std::setw(10) << read_count.load() << " ops\n";
    std::cout << "    Avg Lat:    " << std::setw(10) << read_avg << " μs\n";
    std::cout << "    Throughput: " << std::setw(8) << std::fixed << std::setprecision(1) 
              << (read_count.load() * 1000000.0 / std::max(read_latency_us.load(), static_cast<uint64_t>(1))) << " ops/sec\n";
    std::cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
  }
};

// =============================================================================
// Benchmark Runner
// =============================================================================

class TemporalGraphBenchmark {
 public:
  explicit TemporalGraphBenchmark(const BenchmarkConfig& config)
      : config_(config) {}
  
  Status Initialize() {
    std::cout << "Initializing benchmark...\n";
    std::cout << "  Data directory: " << config_.data_dir << "\n";
    std::cout << "  Vertices: " << config_.num_vertices << "\n";
    std::cout << "  Edges: " << config_.num_edges << "\n";
    std::cout << "  Timestamps: " << config_.num_timestamps << "\n";
    std::cout << "  Threads: " << config_.num_threads << "\n\n";
    
    // Clean up and create directory
    std::filesystem::remove_all(config_.data_dir);
    std::filesystem::create_directories(config_.data_dir);
    
    // Initialize storage
    CedarOptions options;
    options.create_if_missing = true;
    
    CedarGraphStorage* storage_ptr = nullptr;
    auto status = CedarGraphStorage::Open(options, config_.data_dir, &storage_ptr);
    if (!status.ok()) {
      return status;
    }
    storage_.reset(storage_ptr);
    
    return Status::OK();
  }
  
  Status Run() {
    std::cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    std::cout << "  Temporal Graph Benchmark\n";
    std::cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n";
    
    // Phase 1: Data Loading
    std::cout << "[Phase 1] Loading temporal graph data...\n";
    auto start = std::chrono::steady_clock::now();
    LoadTemporalData();
    auto end = std::chrono::steady_clock::now();
    auto load_time = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "  Data loaded in " << load_time << " seconds\n\n";
    
    // Phase 2: Write Benchmark
    std::cout << "[Phase 2] Running write benchmark...\n";
    RunWriteBenchmark();
    
    // Phase 3: Point Read Benchmark
    std::cout << "[Phase 3] Running point read benchmark...\n";
    RunReadBenchmark();
    
    // Print results
    stats_.Print();
    
    return Status::OK();
  }
  
  void Shutdown() {
    storage_.reset();
  }

 private:
  void LoadTemporalData() {
    std::cout << "  Loading vertices...\n";
    
    // Single-threaded loading to avoid concurrency issues
    for (uint64_t i = 0; i < config_.num_vertices; i++) {
      for (uint64_t t = 0; t < config_.num_timestamps; t++) {
        Descriptor desc = Descriptor::InlineInt(0, static_cast<int64_t>(i + t));
        auto status = storage_->Put(i, t * 1000, desc, 1);
        if (!status.ok()) {
          std::cerr << "Put failed: " << status.ToString() << "\n";
          return;
        }
        stats_.bytes_written.fetch_add(8);
      }
      
      if ((i + 1) % 10000 == 0) {
        std::cout << "  Progress: " << (i + 1) << "/" << config_.num_vertices << " vertices\n";
      }
    }
    
    std::cout << "  Progress: " << config_.num_vertices << "/" << config_.num_vertices << " vertices\n";
    std::cout << "  Loading edges...\n";
    
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint64_t> vertex_dist(0, config_.num_vertices - 1);
    
    for (uint64_t i = 0; i < config_.num_edges; i++) {
      for (uint64_t t = 0; t < config_.num_timestamps / 10; t++) {
        Descriptor desc = Descriptor::InlineFloat(1, static_cast<float>(i % 100) / 10.0f);
        uint64_t src = vertex_dist(rng);
        uint64_t dst = vertex_dist(rng);
        auto status = storage_->PutEdge(src, dst, 0, t * 100, desc, 1);
        if (!status.ok()) {
          std::cerr << "PutEdge failed: " << status.ToString() << "\n";
          return;
        }
        stats_.bytes_written.fetch_add(8);
      }
      
      if ((i + 1) % 100000 == 0) {
        std::cout << "  Progress: " << (i + 1) << "/" << config_.num_edges << " edges\n";
      }
    }
    
    std::cout << "  Progress: " << config_.num_edges << "/" << config_.num_edges << " edges\n";
  }
  
  void RunWriteBenchmark() {
    std::mt19937 rng(1000);
    std::uniform_int_distribution<uint64_t> key_dist(0, config_.num_vertices - 1);
    std::uniform_int_distribution<uint64_t> ts_dist(0, config_.num_timestamps * 1000);
    
    auto start = std::chrono::steady_clock::now();
    
    for (uint64_t i = 0; i < config_.write_iterations; i++) {
      uint64_t entity_id = key_dist(rng);
      uint64_t timestamp = ts_dist(rng);
      
      Descriptor desc = Descriptor::InlineInt(0, static_cast<int64_t>(entity_id));
      
      auto op_start = std::chrono::high_resolution_clock::now();
      auto status = storage_->Put(entity_id, timestamp, desc, 1);
      auto op_end = std::chrono::high_resolution_clock::now();
      
      if (status.ok()) {
        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(op_end - op_start).count();
        stats_.write_count.fetch_add(1);
        stats_.write_latency_us.fetch_add(static_cast<uint64_t>(latency));
      }
    }
    
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "  Completed in " << duration << " seconds\n\n";
  }
  
  void RunReadBenchmark() {
    std::mt19937 rng(2000);
    std::uniform_int_distribution<uint64_t> key_dist(0, config_.num_vertices - 1);
    std::uniform_int_distribution<uint64_t> ts_dist(0, config_.num_timestamps * 1000);
    
    auto start = std::chrono::steady_clock::now();
    
    for (uint64_t i = 0; i < config_.read_iterations; i++) {
      uint64_t entity_id = key_dist(rng);
      uint64_t timestamp = ts_dist(rng);
      
      auto op_start = std::chrono::high_resolution_clock::now();
      auto result = storage_->Get(entity_id, timestamp);
      auto op_end = std::chrono::high_resolution_clock::now();
      
      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(op_end - op_start).count();
      stats_.read_count.fetch_add(1);
      stats_.read_latency_us.fetch_add(static_cast<uint64_t>(latency));
      
      if (result.has_value()) {
        stats_.bytes_read.fetch_add(8);
      }
    }
    
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "  Completed in " << duration << " seconds\n\n";
  }
  
  BenchmarkConfig config_;
  std::unique_ptr<CedarGraphStorage> storage_;
  BenchmarkStats stats_;
};

// =============================================================================
// Main
// =============================================================================

int main(int argc, char* argv[]) {
  BenchmarkConfig config;
  
  // Parse command line arguments
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--vertices" && i + 1 < argc) {
      config.num_vertices = std::stoul(argv[++i]);
    } else if (arg == "--edges" && i + 1 < argc) {
      config.num_edges = std::stoul(argv[++i]);
    } else if (arg == "--threads" && i + 1 < argc) {
      config.num_threads = std::stoul(argv[++i]);
    } else if (arg == "--writes" && i + 1 < argc) {
      config.write_iterations = std::stoul(argv[++i]);
    } else if (arg == "--reads" && i + 1 < argc) {
      config.read_iterations = std::stoul(argv[++i]);
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Temporal Graph Benchmark\n"
                << "Usage: " << argv[0] << " [options]\n"
                << "\nOptions:\n"
                << "  --vertices <n>    Number of vertices (default: 100000)\n"
                << "  --edges <n>       Number of edges (default: 500000)\n"
                << "  --threads <n>     Number of threads (default: 4)\n"
                << "  --writes <n>      Write operations (default: 10000)\n"
                << "  --reads <n>       Read operations (default: 100000)\n"
                << "  --help            Show this help\n";
      return 0;
    }
  }
  
  TemporalGraphBenchmark benchmark(config);
  
  auto status = benchmark.Initialize();
  if (!status.ok()) {
    std::cerr << "Failed to initialize benchmark: " << status.ToString() << "\n";
    return 1;
  }
  
  status = benchmark.Run();
  if (!status.ok()) {
    std::cerr << "Benchmark failed: " << status.ToString() << "\n";
    return 1;
  }
  
  benchmark.Shutdown();
  
  return 0;
}
