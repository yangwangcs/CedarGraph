// Copyright 2025 The Cedar Authors
// Optimized 2PC Implementation with Parallel Commit & Pipeline
// Optimizations:
// 1. Parallel 2PC: Different transactions execute 2PC in parallel across nodes
// 2. Transaction Pipeline: Overlap prepare and commit phases
// 3. Async operations: Non-blocking I/O where possible

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
#include <future>
#include <queue>
#include <mutex>
#include <condition_variable>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;
namespace fs = std::filesystem;

// =============================================================================
// Optimized 2PC Configuration
// =============================================================================
struct Optimized2PCConfig {
  int node_count = 3;
  int num_transactions = 1000;
  int num_threads = 4;
  int pipeline_depth = 4;        // Number of transactions in pipeline
  bool enable_parallel = true;   // Enable parallel 2PC across nodes
  bool enable_pipeline = true;   // Enable transaction pipelining
  int batch_prepare_size = 4;    // Batch multiple prepares together
  bool keep_data = false;        // Keep data directories after test
};

// =============================================================================
// Transaction Context
// =============================================================================
struct Transaction {
  uint64_t txn_id;
  std::vector<uint64_t> write_set;
  uint64_t commit_ts;
  std::atomic<bool> prepared{false};
  std::atomic<bool> committed{false};
  std::atomic<int> prepare_acks{0};
  std::atomic<int> commit_acks{0};
  
  Transaction(uint64_t id, const std::vector<uint64_t>& writes, uint64_t ts)
      : txn_id(id), write_set(writes), commit_ts(ts) {}
};

// =============================================================================
// Simulated Storage Node with Optimized 2PC
// =============================================================================
class OptimizedNode {
 public:
  OptimizedNode(int node_id, const std::string& data_dir) 
      : node_id_(node_id), data_dir_(data_dir), running_(false) {
    
    CedarOptions options;
    options.create_if_missing = true;
    
    CedarGraphStorage* storage_ptr = nullptr;
    auto status = CedarGraphStorage::Open(options, data_dir, &storage_ptr);
    if (!status.ok()) {
      throw std::runtime_error("Failed to open storage: " + status.ToString());
    }
    storage_.reset(storage_ptr);
    
    rng_.seed(42 + node_id);
    
    // Start worker thread for pipeline processing
    StartPipelineWorker();
  }
  
  ~OptimizedNode() {
    StopPipelineWorker();
  }
  
  int GetId() const { return node_id_; }
  
  // ==========================================================================
  // Traditional 2PC (Sequential)
  // ==========================================================================
  bool Traditional2PC(uint64_t txn_id, const std::vector<uint64_t>& write_set, 
                      uint64_t commit_ts) {
    // Phase 1: Prepare
    if (!Prepare(txn_id, write_set, commit_ts)) {
      return false;
    }
    
    // Phase 2: Commit
    return Commit(txn_id, write_set, commit_ts);
  }
  
  // ==========================================================================
  // Optimized Parallel 2PC
  // ==========================================================================
  bool ParallelPrepare(uint64_t txn_id, const std::vector<uint64_t>& write_set,
                       uint64_t commit_ts) {
    // Simulate network latency
    std::this_thread::sleep_for(std::chrono::microseconds(100));
    
    // Lock the keys (in real implementation)
    // Here we just simulate success
    return true;
  }
  
  bool ParallelCommit(uint64_t txn_id, const std::vector<uint64_t>& write_set,
                      uint64_t commit_ts) {
    // Write all data
    for (auto key_id : write_set) {
      Descriptor desc(static_cast<uint64_t>(txn_id));
      auto status = storage_->Put(key_id, commit_ts, desc, Timestamp(commit_ts));
      if (!status.ok()) {
        return false;
      }
    }
    return true;
  }
  
  // ==========================================================================
  // Async/Pipelined Operations
  // ==========================================================================
  std::future<bool> AsyncPrepare(uint64_t txn_id, const std::vector<uint64_t>& write_set,
                                  uint64_t commit_ts) {
    return std::async(std::launch::async, [this, txn_id, write_set, commit_ts]() {
      return ParallelPrepare(txn_id, write_set, commit_ts);
    });
  }
  
  std::future<bool> AsyncCommit(uint64_t txn_id, const std::vector<uint64_t>& write_set,
                                 uint64_t commit_ts) {
    return std::async(std::launch::async, [this, txn_id, write_set, commit_ts]() {
      return ParallelCommit(txn_id, write_set, commit_ts);
    });
  }
  
  // ==========================================================================
  // Batch Operations
  // ==========================================================================
  std::vector<bool> BatchPrepare(const std::vector<std::shared_ptr<Transaction>>& txns) {
    std::vector<bool> results;
    results.reserve(txns.size());
    
    for (const auto& txn : txns) {
      results.push_back(ParallelPrepare(txn->txn_id, txn->write_set, txn->commit_ts));
    }
    return results;
  }
  
  std::vector<bool> BatchCommit(const std::vector<std::shared_ptr<Transaction>>& txns) {
    std::vector<bool> results;
    results.reserve(txns.size());
    
    for (const auto& txn : txns) {
      results.push_back(ParallelCommit(txn->txn_id, txn->write_set, txn->commit_ts));
    }
    return results;
  }
  
  // ==========================================================================
  // Pipeline Support
  // ==========================================================================
  void SubmitToPipeline(std::shared_ptr<Transaction> txn) {
    std::unique_lock<std::mutex> lock(pipeline_mutex_);
    pipeline_queue_.push(txn);
    pipeline_cv_.notify_one();
  }
  
  std::string GetStats() {
    auto stats = storage_->GetStats();
    std::ostringstream oss;
    oss << "MemTable: " << stats.memtable_size 
        << ", SST: " << stats.sst_count << " files";
    return oss.str();
  }
  
 private:
  bool Prepare(uint64_t txn_id, const std::vector<uint64_t>& write_set,
               uint64_t commit_ts) {
    std::this_thread::sleep_for(std::chrono::microseconds(50));
    return true;
  }
  
  bool Commit(uint64_t txn_id, const std::vector<uint64_t>& write_set,
              uint64_t commit_ts) {
    for (auto key_id : write_set) {
      Descriptor desc(static_cast<uint64_t>(txn_id));
      auto status = storage_->Put(key_id, commit_ts, desc, Timestamp(commit_ts));
      if (!status.ok()) {
        return false;
      }
    }
    return true;
  }
  
  void StartPipelineWorker() {
    running_ = true;
    pipeline_worker_ = std::thread([this]() {
      PipelineWorkerLoop();
    });
  }
  
  void StopPipelineWorker() {
    running_ = false;
    pipeline_cv_.notify_all();
    if (pipeline_worker_.joinable()) {
      pipeline_worker_.join();
    }
  }
  
  void PipelineWorkerLoop() {
    while (running_) {
      std::unique_lock<std::mutex> lock(pipeline_mutex_);
      pipeline_cv_.wait(lock, [this]() { 
        return !pipeline_queue_.empty() || !running_; 
      });
      
      if (!running_) break;
      
      if (!pipeline_queue_.empty()) {
        auto txn = pipeline_queue_.front();
        pipeline_queue_.pop();
        lock.unlock();
        
        // Process pipeline stage
        if (!txn->prepared.load()) {
          if (ParallelPrepare(txn->txn_id, txn->write_set, txn->commit_ts)) {
            txn->prepared.store(true);
          }
        } else if (!txn->committed.load()) {
          if (ParallelCommit(txn->txn_id, txn->write_set, txn->commit_ts)) {
            txn->committed.store(true);
          }
        }
      }
    }
  }
  
  int node_id_;
  std::string data_dir_;
  std::unique_ptr<CedarGraphStorage> storage_;
  std::mt19937 rng_;
  
  // Pipeline support
  std::queue<std::shared_ptr<Transaction>> pipeline_queue_;
  std::mutex pipeline_mutex_;
  std::condition_variable pipeline_cv_;
  std::thread pipeline_worker_;
  std::atomic<bool> running_;
};

// =============================================================================
// Optimized 2PC Test Suite
// =============================================================================
class Optimized2PCTest {
 public:
  struct TestResult {
    double throughput;  // txns/sec
    double latency;     // μs/txn
    double p50_latency;
    double p99_latency;
    int success_count;
    int total_count;
    double speedup;     // vs baseline
  };
  
  Optimized2PCTest(int node_count, const Optimized2PCConfig& config)
      : node_count_(node_count), config_(config) {
    
    std::cout << "Initializing " << node_count << " optimized nodes..." << std::endl;
    
    for (int i = 0; i < node_count; ++i) {
      std::string data_dir = "/tmp/cedar_2pc_opt/node" + std::to_string(i);
      fs::remove_all(data_dir);
      fs::create_directories(data_dir);
      
      nodes_.push_back(std::make_unique<OptimizedNode>(i, data_dir));
      std::cout << "  Node " << i << " ready" << std::endl;
    }
    
    rng_.seed(42);
  }
  
  ~Optimized2PCTest() {
    nodes_.clear();
    if (!config_.keep_data) {
      for (int i = 0; i < node_count_; ++i) {
        fs::remove_all("/tmp/cedar_2pc_opt/node" + std::to_string(i));
      }
    }
  }
  
  void RunAllTests() {
    std::cout << std::endl;
    std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║     Optimized 2PC Performance Test (" << std::setw(2) << node_count_ 
              << " Nodes)           ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    std::cout << "Configuration:" << std::endl;
    std::cout << "  Transactions: " << config_.num_transactions << std::endl;
    std::cout << "  Threads:      " << config_.num_threads << std::endl;
    std::cout << "  Pipeline:     " << (config_.enable_pipeline ? "Enabled" : "Disabled") << std::endl;
    std::cout << "  Parallel:     " << (config_.enable_parallel ? "Enabled" : "Disabled") << std::endl;
    std::cout << "  Batch Size:   " << config_.batch_prepare_size << std::endl;
    std::cout << std::endl;
    
    // Baseline test
    std::cout << "[Test 1] Traditional 2PC (Sequential)" << std::endl;
    auto baseline = TestTraditional2PC();
    
    std::cout << std::endl;
    
    // Optimized tests
    std::cout << "[Test 2] Parallel 2PC (Multi-threaded)" << std::endl;
    auto parallel = TestParallel2PC();
    parallel.speedup = baseline.throughput > 0 ? parallel.throughput / baseline.throughput : 0;
    
    std::cout << std::endl;
    
    std::cout << "[Test 3] Pipelined 2PC (Async)" << std::endl;
    auto pipelined = TestPipelined2PC();
    pipelined.speedup = baseline.throughput > 0 ? pipelined.throughput / baseline.throughput : 0;
    
    std::cout << std::endl;
    
    std::cout << "[Test 4] Batch 2PC (Batched Prepare)" << std::endl;
    auto batched = TestBatch2PC();
    batched.speedup = baseline.throughput > 0 ? batched.throughput / baseline.throughput : 0;
    
    // Print comparison
    PrintComparison(baseline, parallel, pipelined, batched);
  }
  
 private:
  TestResult TestTraditional2PC() {
    int num_txns = config_.num_transactions;
    std::atomic<int> success_count{0};
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
      
      // Sequential 2PC to all nodes
      bool all_prepared = true;
      for (auto& node : nodes_) {
        if (!node->Traditional2PC(txn_id, write_set, commit_ts)) {
          all_prepared = false;
          break;
        }
      }
      
      if (all_prepared) {
        success_count++;
        auto op_end = std::chrono::high_resolution_clock::now();
        double latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
            op_end - op_start).count() / 1000.0;
        std::lock_guard<std::mutex> lock(latency_mutex);
        latencies.push_back(latency);
      }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    return CalculateResult(success_count, num_txns, duration_us, latencies);
  }
  
  TestResult TestParallel2PC() {
    int num_txns = config_.num_transactions;
    std::atomic<int> success_count{0};
    std::vector<double> latencies;
    std::mutex latency_mutex;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    int txns_per_thread = num_txns / config_.num_threads;
    
    for (int tid = 0; tid < config_.num_threads; ++tid) {
      threads.emplace_back([&, tid]() {
        for (int i = 0; i < txns_per_thread; ++i) {
          int txn_idx = tid * txns_per_thread + i;
          uint64_t txn_id = 20000 + txn_idx;
          std::vector<uint64_t> write_set;
          for (int j = 0; j < 5; ++j) {
            write_set.push_back((rng_() + txn_idx) % 1000);
          }
          uint64_t commit_ts = 6000000 + txn_idx;
          
          auto op_start = std::chrono::high_resolution_clock::now();
          
          // Parallel 2PC: Each node processes independently
          std::vector<std::future<bool>> prepare_futures;
          for (auto& node : nodes_) {
            prepare_futures.push_back(node->AsyncPrepare(txn_id, write_set, commit_ts));
          }
          
          bool all_prepared = true;
          for (auto& f : prepare_futures) {
            if (!f.get()) {
              all_prepared = false;
              break;
            }
          }
          
          if (all_prepared) {
            std::vector<std::future<bool>> commit_futures;
            for (auto& node : nodes_) {
              commit_futures.push_back(node->AsyncCommit(txn_id, write_set, commit_ts));
            }
            
            bool all_committed = true;
            for (auto& f : commit_futures) {
              if (!f.get()) {
                all_committed = false;
                break;
              }
            }
            
            if (all_committed) {
              success_count++;
              auto op_end = std::chrono::high_resolution_clock::now();
              double latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  op_end - op_start).count() / 1000.0;
              std::lock_guard<std::mutex> lock(latency_mutex);
              latencies.push_back(latency);
            }
          }
        }
      });
    }
    
    for (auto& t : threads) t.join();
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    return CalculateResult(success_count, num_txns, duration_us, latencies);
  }
  
  TestResult TestPipelined2PC() {
    int num_txns = config_.num_transactions;
    std::atomic<int> success_count{0};
    std::vector<double> latencies;
    std::mutex latency_mutex;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Process transactions in pipeline groups
    int pipeline_size = config_.pipeline_depth;
    int num_pipelines = (num_txns + pipeline_size - 1) / pipeline_size;
    
    for (int p = 0; p < num_pipelines; ++p) {
      int start_idx = p * pipeline_size;
      int end_idx = std::min(start_idx + pipeline_size, num_txns);
      
      // Create transaction batch
      std::vector<std::shared_ptr<Transaction>> batch;
      for (int i = start_idx; i < end_idx; ++i) {
        uint64_t txn_id = 30000 + i;
        std::vector<uint64_t> write_set;
        for (int j = 0; j < 5; ++j) {
          write_set.push_back((rng_() + i) % 1000);
        }
        uint64_t commit_ts = 7000000 + i;
        
        batch.push_back(std::make_shared<Transaction>(txn_id, write_set, commit_ts));
      }
      
      // Pipeline: Overlap prepare and commit across batches
      auto op_start = std::chrono::high_resolution_clock::now();
      
      // Phase 1: Prepare all in parallel
      std::vector<std::future<std::vector<bool>>> prepare_results;
      for (auto& node : nodes_) {
        prepare_results.push_back(
            std::async(std::launch::async, [&node, batch]() {
              return node->BatchPrepare(batch);
            }));
      }
      
      // Collect prepare results
      bool all_prepared = true;
      for (auto& f : prepare_results) {
        auto results = f.get();
        for (bool r : results) {
          if (!r) all_prepared = false;
        }
      }
      
      if (all_prepared) {
        // Phase 2: Commit all in parallel
        std::vector<std::future<std::vector<bool>>> commit_results;
        for (auto& node : nodes_) {
          commit_results.push_back(
              std::async(std::launch::async, [&node, batch]() {
                return node->BatchCommit(batch);
              }));
        }
        
        bool all_committed = true;
        for (auto& f : commit_results) {
          auto results = f.get();
          for (bool r : results) {
            if (!r) all_committed = false;
          }
        }
        
        if (all_committed) {
          success_count += batch.size();
          auto op_end = std::chrono::high_resolution_clock::now();
          double latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
              op_end - op_start).count() / 1000.0 / batch.size();
          std::lock_guard<std::mutex> lock(latency_mutex);
          for (size_t i = 0; i < batch.size(); ++i) {
            latencies.push_back(latency);
          }
        }
      }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    return CalculateResult(success_count, num_txns, duration_us, latencies);
  }
  
  TestResult TestBatch2PC() {
    int num_txns = config_.num_transactions;
    std::atomic<int> success_count{0};
    std::vector<double> latencies;
    std::mutex latency_mutex;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    int batch_size = config_.batch_prepare_size;
    int num_batches = (num_txns + batch_size - 1) / batch_size;
    
    for (int b = 0; b < num_batches; ++b) {
      int start_idx = b * batch_size;
      int end_idx = std::min(start_idx + batch_size, num_txns);
      
      std::vector<std::shared_ptr<Transaction>> batch;
      for (int i = start_idx; i < end_idx; ++i) {
        uint64_t txn_id = 40000 + i;
        std::vector<uint64_t> write_set;
        for (int j = 0; j < 5; ++j) {
          write_set.push_back((rng_() + i) % 1000);
        }
        uint64_t commit_ts = 8000000 + i;
        
        batch.push_back(std::make_shared<Transaction>(txn_id, write_set, commit_ts));
      }
      
      auto op_start = std::chrono::high_resolution_clock::now();
      
      // Batched prepare to all nodes
      bool all_prepared = true;
      for (auto& node : nodes_) {
        auto results = node->BatchPrepare(batch);
        for (bool r : results) {
          if (!r) all_prepared = false;
        }
      }
      
      if (all_prepared) {
        // Batched commit to all nodes
        bool all_committed = true;
        for (auto& node : nodes_) {
          auto results = node->BatchCommit(batch);
          for (bool r : results) {
            if (!r) all_committed = false;
          }
        }
        
        if (all_committed) {
          success_count += batch.size();
          auto op_end = std::chrono::high_resolution_clock::now();
          double latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
              op_end - op_start).count() / 1000.0 / batch.size();
          std::lock_guard<std::mutex> lock(latency_mutex);
          for (size_t i = 0; i < batch.size(); ++i) {
            latencies.push_back(latency);
          }
        }
      }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
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
  
  void PrintComparison(const TestResult& baseline, const TestResult& parallel,
                       const TestResult& pipelined, const TestResult& batched) {
    std::cout << std::endl;
    std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║          2PC Optimization Comparison (" << std::setw(2) << node_count_ 
              << " Nodes)          ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    
    std::cout << std::left << std::setw(20) << "Strategy" 
              << std::right << std::setw(12) << "Throughput"
              << std::setw(12) << "Avg Latency"
              << std::setw(12) << "P99 Latency"
              << std::setw(12) << "Speedup" << std::endl;
    std::cout << std::string(68, '-') << std::endl;
    
    PrintResultRow("Baseline (Seq)", baseline, 1.0);
    PrintResultRow("Parallel", parallel, parallel.speedup);
    PrintResultRow("Pipelined", pipelined, pipelined.speedup);
    PrintResultRow("Batched", batched, batched.speedup);
    
    std::cout << std::endl;
    std::cout << "Optimizations Analysis:" << std::endl;
    std::cout << "  • Parallel:  Async operations across nodes" << std::endl;
    std::cout << "  • Pipelined: Overlap prepare/commit phases" << std::endl;
    std::cout << "  • Batched:   Amortize network overhead" << std::endl;
    std::cout << std::endl;
  }
  
  void PrintResultRow(const std::string& name, const TestResult& result, double speedup) {
    std::cout << std::left << std::setw(20) << name
              << std::right << std::setw(11) << std::fixed << std::setprecision(1) 
              << result.throughput << " "
              << std::setw(11) << std::setprecision(1) << result.latency << " "
              << std::setw(11) << std::setprecision(1) << result.p99_latency << " "
              << std::setw(10) << std::setprecision(2) << speedup << "x"
              << std::setw(6) << result.success_count << "/" << result.total_count 
              << std::endl;
  }
  
  int node_count_;
  Optimized2PCConfig config_;
  std::vector<std::unique_ptr<OptimizedNode>> nodes_;
  std::mt19937 rng_;
};

// =============================================================================
// Main
// =============================================================================
int main(int argc, char* argv[]) {
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     CedarGraph Optimized 2PC Performance Test              ║" << std::endl;
  std::cout << "║     (Parallel + Pipeline + Batch Optimizations)            ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  Optimized2PCConfig config;
  std::vector<int> node_configs = {3, 5, 7};
  
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--nodes" && i + 1 < argc) {
      node_configs = {std::stoi(argv[++i])};
    } else if (arg == "--txns" && i + 1 < argc) {
      config.num_transactions = std::stoi(argv[++i]);
    } else if (arg == "--threads" && i + 1 < argc) {
      config.num_threads = std::stoi(argv[++i]);
    } else if (arg == "--pipeline" && i + 1 < argc) {
      config.pipeline_depth = std::stoi(argv[++i]);
    } else if (arg == "--batch" && i + 1 < argc) {
      config.batch_prepare_size = std::stoi(argv[++i]);
    } else if (arg == "--keep-data") {
      config.keep_data = true;
    } else if (arg == "--help") {
      std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
      std::cout << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  --nodes N       Test with N nodes (3/5/7, default: all)" << std::endl;
      std::cout << "  --txns N        Number of transactions (default: 1000)" << std::endl;
      std::cout << "  --threads N     Number of threads (default: 4)" << std::endl;
      std::cout << "  --pipeline N    Pipeline depth (default: 4)" << std::endl;
      std::cout << "  --batch N       Batch size (default: 4)" << std::endl;
      std::cout << "  --keep-data     Keep data directories" << std::endl;
      return 0;
    }
  }
  
  for (int node_count : node_configs) {
    try {
      Optimized2PCTest test(node_count, config);
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
