// Copyright 2025 The Cedar Authors
// Multi-Node Performance Test (Local Simulation)
// Simulates 3/5/7 node clusters using separate storage directories

#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <thread>
#include <atomic>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <memory>
#include <filesystem>
#include <cmath>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;
namespace fs = std::filesystem;

// =============================================================================
// Test Configuration
// =============================================================================
struct TestConfig {
  int node_count = 3;
  int num_operations = 10000;
  int num_threads = 4;
  int batch_size = 100;
  int warmup_ops = 1000;
  bool cleanup_after = true;
};

struct TestResult {
  double throughput;  // ops/sec
  double latency;     // μs/op
  double p50_latency;
  double p99_latency;
  int success_count;
  int total_count;
};

// =============================================================================
// Simulated Storage Node
// =============================================================================
class SimulatedNode {
 public:
  SimulatedNode(int node_id, const std::string& data_dir) 
      : node_id_(node_id), data_dir_(data_dir) {
    
    // Create storage options
    CedarOptions options;
    options.create_if_missing = true;
    
    // Open storage using factory method
    CedarGraphStorage* storage_ptr = nullptr;
    auto status = CedarGraphStorage::Open(options, data_dir, &storage_ptr);
    if (!status.ok()) {
      throw std::runtime_error("Failed to open storage: " + status.ToString());
    }
    storage_.reset(storage_ptr);
    
    // Random seed per node
    rng_.seed(42 + node_id);
  }
  
  ~SimulatedNode() = default;
  
  int GetId() const { return node_id_; }
  
  // Single write
  bool Write(uint64_t key_id, int32_t value, uint64_t timestamp) {
    Descriptor desc(static_cast<uint64_t>(value));
    auto status = storage_->Put(key_id, timestamp, desc, Timestamp(timestamp));
    return status.ok();
  }
  
  // Single read - using Scan to get value at specific time
  std::optional<int32_t> Read(uint64_t key_id, uint64_t timestamp) {
    // Use Scan to get the value at a specific time
    auto results = storage_->Scan(key_id, Timestamp(0), Timestamp(timestamp));
    if (!results.empty()) {
      // Return the most recent value (last in the list due to descending time order)
      return static_cast<int32_t>(results.back().second.AsRaw());
    }
    return std::nullopt;
  }
  
  // Batch write
  bool BatchWrite(const std::vector<std::pair<uint64_t, int32_t>>& items, uint64_t timestamp) {
    for (const auto& [key_id, value] : items) {
      Descriptor desc(static_cast<uint64_t>(value));
      auto status = storage_->Put(key_id, timestamp, desc, Timestamp(timestamp));
      if (!status.ok()) {
        return false;
      }
    }
    return true;
  }
  
  // Simulated 2PC Prepare
  bool Prepare(uint64_t txn_id, const std::vector<uint64_t>& write_set, uint64_t commit_ts) {
    // In real implementation, this would check locks
    // Here we just simulate the network delay
    std::this_thread::sleep_for(std::chrono::microseconds(50));
    return true;
  }
  
  // Simulated 2PC Commit
  bool Commit(uint64_t txn_id, const std::vector<uint64_t>& write_set, uint64_t commit_ts) {
    // Write all items
    for (auto key_id : write_set) {
      Descriptor desc(static_cast<uint64_t>(txn_id));
      auto status = storage_->Put(key_id, commit_ts, desc, Timestamp(commit_ts));
      if (!status.ok()) {
        return false;
      }
    }
    return true;
  }
  
  // Get storage stats
  std::string GetStats() {
    auto stats = storage_->GetStats();
    std::ostringstream oss;
    oss << "MemTable: " << stats.memtable_size 
        << ", SST: " << stats.sst_count << " files, " << stats.sst_size << " bytes";
    return oss.str();
  }
  
 private:
  int node_id_;
  std::string data_dir_;
  std::unique_ptr<CedarGraphStorage> storage_;
  std::mt19937 rng_;
};

// =============================================================================
// Multi-Node Test Suite
// =============================================================================
class MultiNodeLocalTest {
 public:
  MultiNodeLocalTest(int node_count, const TestConfig& config)
      : node_count_(node_count), config_(config) {
    
    std::cout << "Initializing " << node_count << " simulated nodes..." << std::endl;
    
    for (int i = 0; i < node_count; ++i) {
      std::string data_dir = "/tmp/cedar_multi_node/node" + std::to_string(i);
      
      // Clean and create directory
      fs::remove_all(data_dir);
      fs::create_directories(data_dir);
      
      nodes_.push_back(std::make_unique<SimulatedNode>(i, data_dir));
      std::cout << "  Node " << i << " initialized at " << data_dir << std::endl;
    }
    
    rng_.seed(42);
    std::cout << "  All " << node_count << " nodes ready" << std::endl;
  }
  
  ~MultiNodeLocalTest() {
    nodes_.clear();  // Release storages first
    
    if (config_.cleanup_after) {
      for (int i = 0; i < node_count_; ++i) {
        std::string data_dir = "/tmp/cedar_multi_node/node" + std::to_string(i);
        fs::remove_all(data_dir);
      }
    }
  }
  
  void RunAllTests() {
    std::cout << std::endl;
    std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║     Multi-Node Performance Test (" << std::setw(2) << node_count_ 
              << " Nodes - Local)         ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    std::cout << "Configuration:" << std::endl;
    std::cout << "  Operations: " << config_.num_operations << std::endl;
    std::cout << "  Threads:    " << config_.num_threads << std::endl;
    std::cout << "  Batch Size: " << config_.batch_size << std::endl;
    std::cout << std::endl;
    
    // Warmup
    std::cout << "[Warming up...]" << std::endl;
    RunWarmup();
    
    // Run tests
    auto write_result = TestWriteThroughput();
    auto read_result = TestReadThroughput();
    auto batch_result = TestBatchWrite();
    auto txn_result = Test2PCTransactions();
    
    // Print summary
    PrintSummary(write_result, read_result, batch_result, txn_result);
    
    // Print node stats
    std::cout << "Node Statistics:" << std::endl;
    for (auto& node : nodes_) {
      std::cout << "  Node " << node->GetId() << ": " << node->GetStats() << std::endl;
    }
  }
  
 private:
  void RunWarmup() {
    for (int i = 0; i < config_.warmup_ops; ++i) {
      uint64_t key = rng_() % 1000;
      int32_t value = rng_();
      nodes_[i % nodes_.size()]->Write(key, value, 1000000 + i);
    }
  }
  
  TestResult TestWriteThroughput() {
    std::cout << "[Test 1] Single Write Throughput" << std::endl;
    
    std::atomic<int> success_count{0};
    std::vector<double> latencies;
    std::mutex latency_mutex;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    int ops_per_thread = config_.num_operations / config_.num_threads;
    
    for (int t = 0; t < config_.num_threads; ++t) {
      threads.emplace_back([&, t]() {
        for (int i = 0; i < ops_per_thread; ++i) {
          uint64_t key = (t * ops_per_thread + i) % 10000;
          int32_t value = rng_();
          uint64_t ts = 2000000 + i;
          int node_idx = (t * ops_per_thread + i) % nodes_.size();
          
          auto op_start = std::chrono::high_resolution_clock::now();
          bool success = nodes_[node_idx]->Write(key, value, ts);
          auto op_end = std::chrono::high_resolution_clock::now();
          
          if (success) {
            success_count++;
            double latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
                op_end - op_start).count() / 1000.0;
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies.push_back(latency);
          }
        }
      });
    }
    
    for (auto& t : threads) t.join();
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    return CalculateResult(success_count, config_.num_operations, duration_us, latencies);
  }
  
  TestResult TestReadThroughput() {
    std::cout << "[Test 2] Single Read Throughput" << std::endl;
    
    // Pre-write data
    std::cout << "  Preparing data..." << std::endl;
    for (int i = 0; i < 5000; ++i) {
      nodes_[i % nodes_.size()]->Write(i, i * 100, 3000000);
    }
    
    std::atomic<int> success_count{0};
    std::vector<double> latencies;
    std::mutex latency_mutex;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    int ops_per_thread = config_.num_operations / config_.num_threads;
    
    for (int t = 0; t < config_.num_threads; ++t) {
      threads.emplace_back([&, t]() {
        for (int i = 0; i < ops_per_thread; ++i) {
          uint64_t key = rng_() % 5000;
          int node_idx = (t * ops_per_thread + i) % nodes_.size();
          
          auto op_start = std::chrono::high_resolution_clock::now();
          auto result = nodes_[node_idx]->Read(key, 3000000);
          auto op_end = std::chrono::high_resolution_clock::now();
          
          if (result.has_value()) {
            success_count++;
            double latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
                op_end - op_start).count() / 1000.0;
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies.push_back(latency);
          }
        }
      });
    }
    
    for (auto& t : threads) t.join();
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    return CalculateResult(success_count, config_.num_operations, duration_us, latencies);
  }
  
  TestResult TestBatchWrite() {
    std::cout << "[Test 3] Batch Write Throughput (batch_size=" 
              << config_.batch_size << ")" << std::endl;
    
    std::atomic<int> success_count{0};
    std::atomic<int> total_batches{0};
    std::vector<double> latencies;
    std::mutex latency_mutex;
    
    int num_batches = config_.num_operations / config_.batch_size;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    int batches_per_thread = num_batches / config_.num_threads;
    
    for (int t = 0; t < config_.num_threads; ++t) {
      threads.emplace_back([&, t]() {
        for (int i = 0; i < batches_per_thread; ++i) {
          std::vector<std::pair<uint64_t, int32_t>> batch;
          for (int j = 0; j < config_.batch_size; ++j) {
            batch.push_back({(t * batches_per_thread + i) * config_.batch_size + j, rng_()});
          }
          
          int node_idx = (t * batches_per_thread + i) % nodes_.size();
          
          auto op_start = std::chrono::high_resolution_clock::now();
          bool success = nodes_[node_idx]->BatchWrite(batch, 4000000 + i);
          auto op_end = std::chrono::high_resolution_clock::now();
          
          total_batches++;
          if (success) {
            success_count++;
            double latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
                op_end - op_start).count() / 1000.0;
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies.push_back(latency);
          }
        }
      });
    }
    
    for (auto& t : threads) t.join();
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    int total_ops = total_batches * config_.batch_size;
    int success_ops = success_count * config_.batch_size;
    
    TestResult result = CalculateResult(success_ops, total_ops, duration_us, latencies);
    result.throughput = (double)success_ops / (duration_us / 1000000.0);
    return result;
  }
  
  TestResult Test2PCTransactions() {
    std::cout << "[Test 4] 2PC Transaction Throughput" << std::endl;
    
    int num_txns = config_.num_operations / 10;
    std::atomic<int> success_count{0};
    std::atomic<int> prepared_count{0};
    std::vector<double> latencies;
    std::mutex latency_mutex;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int t = 0; t < num_txns; ++t) {
      uint64_t txn_id = 10000 + t;
      std::vector<uint64_t> write_set;
      
      for (int i = 0; i < 5; ++i) {
        write_set.push_back(rng_() % 1000);
      }
      
      uint64_t commit_ts = 5000000 + t;
      
      auto op_start = std::chrono::high_resolution_clock::now();
      
      // Phase 1: Prepare (to all nodes)
      int prepare_ok = 0;
      for (auto& node : nodes_) {
        if (node->Prepare(txn_id, write_set, commit_ts)) {
          prepare_ok++;
        }
      }
      
      if (prepare_ok == nodes_.size()) {
        prepared_count++;
        
        // Phase 2: Commit (to all nodes)
        int commit_ok = 0;
        for (auto& node : nodes_) {
          if (node->Commit(txn_id, write_set, commit_ts)) {
            commit_ok++;
          }
        }
        
        if (commit_ok == nodes_.size()) {
          success_count++;
          auto op_end = std::chrono::high_resolution_clock::now();
          double latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
              op_end - op_start).count() / 1000.0;
          std::lock_guard<std::mutex> lock(latency_mutex);
          latencies.push_back(latency);
        }
      }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    std::cout << "  Prepared: " << prepared_count << "/" << num_txns << std::endl;
    
    return CalculateResult(success_count, num_txns, duration_us, latencies);
  }
  
  TestResult CalculateResult(int success, int total, int64_t duration_us, 
                             std::vector<double>& latencies) {
    TestResult result;
    result.success_count = success;
    result.total_count = total;
    result.throughput = (double)success / (duration_us / 1000000.0);
    result.latency = latencies.empty() ? 0.0 : 
                     std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    
    if (!latencies.empty()) {
      std::sort(latencies.begin(), latencies.end());
      result.p50_latency = latencies[latencies.size() * 0.5];
      result.p99_latency = latencies[latencies.size() * 0.99];
    }
    
    return result;
  }
  
  void PrintSummary(const TestResult& write, const TestResult& read, 
                    const TestResult& batch, const TestResult& txn) {
    std::cout << std::endl;
    std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║           Performance Summary (" << std::setw(2) << node_count_ 
              << " Nodes)                 ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    
    std::cout << std::left << std::setw(20) << "Test" 
              << std::right << std::setw(12) << "Throughput"
              << std::setw(12) << "Avg Latency"
              << std::setw(12) << "P50 Latency"
              << std::setw(12) << "P99 Latency"
              << std::setw(10) << "Success" << std::endl;
    std::cout << std::string(76, '-') << std::endl;
    
    PrintResultRow("Write (ops/sec)", write);
    PrintResultRow("Read (ops/sec)", read);
    PrintResultRow("Batch (ops/sec)", batch);
    PrintResultRow("2PC (txns/sec)", txn);
    
    std::cout << std::endl;
  }
  
  void PrintResultRow(const std::string& name, const TestResult& result) {
    std::cout << std::left << std::setw(20) << name
              << std::right << std::setw(11) << std::fixed << std::setprecision(1) 
              << result.throughput << " "
              << std::setw(11) << std::setprecision(1) << result.latency << " "
              << std::setw(11) << std::setprecision(1) << result.p50_latency << " "
              << std::setw(11) << std::setprecision(1) << result.p99_latency << " "
              << std::setw(8) << result.success_count << "/" << result.total_count 
              << std::endl;
  }
  
  int node_count_;
  TestConfig config_;
  std::vector<std::unique_ptr<SimulatedNode>> nodes_;
  std::mt19937 rng_;
};

// =============================================================================
// Main
// =============================================================================
int main(int argc, char* argv[]) {
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     CedarGraph Multi-Node Performance Test (Local)         ║" << std::endl;
  std::cout << "║              (3-node / 5-node / 7-node)                    ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  TestConfig config;
  std::vector<int> node_configs = {3, 5, 7};
  
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--nodes" && i + 1 < argc) {
      node_configs = {std::stoi(argv[++i])};
    } else if (arg == "--ops" && i + 1 < argc) {
      config.num_operations = std::stoi(argv[++i]);
    } else if (arg == "--threads" && i + 1 < argc) {
      config.num_threads = std::stoi(argv[++i]);
    } else if (arg == "--keep-data") {
      config.cleanup_after = false;
    } else if (arg == "--help") {
      std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
      std::cout << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  --nodes N       Test with N nodes (3/5/7, default: all)" << std::endl;
      std::cout << "  --ops N         Number of operations (default: 10000)" << std::endl;
      std::cout << "  --threads N     Number of threads (default: 4)" << std::endl;
      std::cout << "  --keep-data     Don't cleanup data directories after test" << std::endl;
      return 0;
    }
  }
  
  for (int node_count : node_configs) {
    try {
      MultiNodeLocalTest test(node_count, config);
      test.RunAllTests();
      
      if (node_count != node_configs.back()) {
        std::cout << std::endl << std::string(60, '=') << std::endl << std::endl;
      }
    } catch (const std::exception& e) {
      std::cerr << "Error: " << e.what() << std::endl;
      return 1;
    }
  }
  
  std::cout << std::endl << "All tests completed!" << std::endl;
  return 0;
}
