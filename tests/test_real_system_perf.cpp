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
// REAL System Performance Test - Using Actual CedarGraph Components
// 真实系统性能测试 - 使用实际的 CedarGraph 组件
// =============================================================================

#include <iostream>
#include <chrono>
#include <iomanip>
#include <random>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <numeric>
#include <filesystem>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/core/slice.h"

using namespace cedar;

// 简化版的性能测试，直接使用 CedarGraphStorage
struct PerformanceMetrics {
  std::atomic<uint64_t> total_ops{0};
  std::atomic<uint64_t> reads{0};
  std::atomic<uint64_t> writes{0};
  std::atomic<uint64_t> range_queries{0};
  std::atomic<uint64_t> failed_ops{0};
  
  std::vector<uint64_t> read_latencies;
  std::vector<uint64_t> write_latencies;
  std::vector<uint64_t> range_latencies;
  mutable std::mutex latency_mutex;
  
  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point end_time;
  
  void Start() { start_time = std::chrono::steady_clock::now(); }
  void Stop() { end_time = std::chrono::steady_clock::now(); }
  
  double GetDurationSeconds() const {
    return std::chrono::duration<double>(end_time - start_time).count();
  }
  
  double GetThroughput() const {
    return total_ops.load() / GetDurationSeconds();
  }
};

class RealStorageBenchmark {
 public:
  RealStorageBenchmark(int duration_sec, int num_threads, 
                       uint64_t num_keys, const std::string& data_dir)
      : duration_sec_(duration_sec), num_threads_(num_threads),
        num_keys_(num_keys), data_dir_(data_dir), stop_(false) {}
  
  ~RealStorageBenchmark() {
    Cleanup();
  }
  
  Status Initialize() {
    std::cout << "Initializing real storage benchmark..." << std::endl;
    
    // 清理并创建数据目录
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);
    
    // 打开 CedarGraphStorage
    CedarOptions options;
    options.create_if_missing = true;
    options.compression = kLZ4Compression;
    options.write_buffer_size = 64 * 1024 * 1024;
    
    CedarGraphStorage* storage = nullptr;
    Status s = CedarGraphStorage::Open(options, data_dir_, &storage);
    if (!s.ok()) {
      return s;
    }
    storage_.reset(storage);
    
    std::cout << "Storage opened at: " << data_dir_ << std::endl;
    
    // 生成测试用的 key
    keys_.reserve(num_keys_);
    for (uint64_t i = 0; i < num_keys_; ++i) {
      keys_.push_back("key_" + std::to_string(i));
    }
    
    return Status::OK();
  }
  
  Status PrepareData() {
    std::cout << "Preloading " << num_keys_ << " keys..." << std::endl;
    
    const size_t BATCH = 10000;
    for (size_t i = 0; i < keys_.size(); i += BATCH) {
      size_t end = std::min(i + BATCH, keys_.size());
      
      for (size_t j = i; j < end; ++j) {
        Slice key(keys_[j]);
        std::string value = GenerateValue(j);
        Slice value_slice(value);
        
        Status s = storage_->Put(WriteOptions(), key, value_slice);
        if (!s.ok()) return s;
      }
      
      if ((i / BATCH) % 10 == 0) {
        std::cout << "  Loaded " << i << " keys..." << std::endl;
      }
    }
    
    storage_->Flush();
    std::cout << "Data preload complete!" << std::endl;
    return Status::OK();
  }
  
  void Run() {
    std::cout << "\nRunning benchmark for " << duration_sec_ << " seconds..." << std::endl;
    
    metrics_.Start();
    stop_ = false;
    
    // 启动工作线程
    std::vector<std::thread> workers;
    for (int i = 0; i < num_threads_; ++i) {
      workers.emplace_back([this, i]() { WorkerThread(i); });
    }
    
    // 进度报告
    std::thread reporter([this]() {
      auto start = std::chrono::steady_clock::now();
      while (!stop_) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start).count();
        
        if (elapsed >= duration_sec_) break;
        
        auto ops = metrics_.total_ops.load();
        auto throughput = ops / std::max(1.0, (double)elapsed);
        
        std::cout << "[Progress] " << elapsed << "s / " << duration_sec_ 
                  << "s | Ops: " << ops 
                  << " | Throughput: " << std::fixed << std::setprecision(1) 
                  << throughput << " ops/s" << std::endl;
      }
    });
    
    // 等待测试完成
    std::this_thread::sleep_for(std::chrono::seconds(duration_sec_));
    stop_ = true;
    
    for (auto& w : workers) {
      w.join();
    }
    
    metrics_.Stop();
    reporter.join();
    
    PrintResults();
  }
  
  void Cleanup() {
    storage_.reset();
    std::filesystem::remove_all(data_dir_);
  }
  
 private:
  void WorkerThread(int thread_id) {
    std::mt19937 rng(std::random_device{}() + thread_id);
    std::uniform_int_distribution<size_t> key_dist(0, keys_.size() - 1);
    std::uniform_int_distribution<int> op_dist(0, 99);
    
    while (!stop_) {
      int op = op_dist(rng);
      
      if (op < 70) {
        // 70% 读操作
        DoRead(key_dist(rng));
      } else if (op < 90) {
        // 20% 写操作
        DoWrite(key_dist(rng));
      } else {
        // 10% 范围查询
        DoRangeQuery();
      }
    }
  }
  
  void DoRead(size_t key_idx) {
    auto start = std::chrono::high_resolution_clock::now();
    
    Slice key(keys_[key_idx]);
    std::string value;
    Status s = storage_->Get(ReadOptions(), key, &value);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    {
      std::lock_guard<std::mutex> lock(metrics_.latency_mutex);
      metrics_.read_latencies.push_back(latency);
    }
    
    if (s.ok()) {
      metrics_.reads.fetch_add(1);
      metrics_.total_ops.fetch_add(1);
    } else {
      metrics_.failed_ops.fetch_add(1);
    }
  }
  
  void DoWrite(size_t key_idx) {
    auto start = std::chrono::high_resolution_clock::now();
    
    Slice key(keys_[key_idx]);
    std::string value = GenerateValue(key_idx);
    Slice value_slice(value);
    
    Status s = storage_->Put(WriteOptions(), key, value_slice);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    {
      std::lock_guard<std::mutex> lock(metrics_.latency_mutex);
      metrics_.write_latencies.push_back(latency);
    }
    
    if (s.ok()) {
      metrics_.writes.fetch_add(1);
      metrics_.total_ops.fetch_add(1);
    } else {
      metrics_.failed_ops.fetch_add(1);
    }
  }
  
  void DoRangeQuery() {
    auto start = std::chrono::high_resolution_clock::now();
    
    auto* iter = storage_->NewIterator(ReadOptions());
    int count = 0;
    
    for (iter->SeekToFirst(); iter->Valid() && count < 1000; iter->Next()) {
      count++;
    }
    
    delete iter;
    
    auto end = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    {
      std::lock_guard<std::mutex> lock(metrics_.latency_mutex);
      metrics_.range_latencies.push_back(latency);
    }
    
    metrics_.range_queries.fetch_add(1);
    metrics_.total_ops.fetch_add(1);
  }
  
  std::string GenerateValue(uint64_t idx) {
    return R"({"id":)" + std::to_string(idx) + 
           R"(,"data":"value_"" + std::to_string(idx) + 
           R"(","timestamp":)" + std::to_string(
               std::chrono::system_clock::now().time_since_epoch().count()) + "}";
  }
  
  void PrintResults() {
    double duration = metrics_.GetDurationSeconds();
    
    std::cout << "\n╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║     REAL CedarGraphStorage Performance Results             ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    
    std::cout << "Duration: " << std::fixed << std::setprecision(2) << duration << " seconds" << std::endl;
    std::cout << "Threads:  " << num_threads_ << std::endl;
    std::cout << std::endl;
    
    std::cout << "Operations:" << std::endl;
    std::cout << "  Total:  " << metrics_.total_ops.load() << std::endl;
    std::cout << "  Reads:  " << metrics_.reads.load() << std::endl;
    std::cout << "  Writes: " << metrics_.writes.load() << std::endl;
    std::cout << "  Range:  " << metrics_.range_queries.load() << std::endl;
    std::cout << "  Failed: " << metrics_.failed_ops.load() << std::endl;
    std::cout << std::endl;
    
    std::cout << "Throughput: " << metrics_.GetThroughput() << " ops/sec" << std::endl;
    std::cout << std::endl;
    
    auto calc_p50 = [](const std::vector<uint64_t>& v) -> uint64_t {
      if (v.empty()) return 0;
      auto sorted = v;
      std::sort(sorted.begin(), sorted.end());
      return sorted[sorted.size() * 0.5];
    };
    
    auto calc_avg = [](const std::vector<uint64_t>& v) -> double {
      if (v.empty()) return 0;
      return std::accumulate(v.begin(), v.end(), 0.0) / v.size();
    };
    
    std::cout << "Latency (microseconds):" << std::endl;
    if (!metrics_.read_latencies.empty()) {
      std::cout << "  Read:   P50=" << calc_p50(metrics_.read_latencies) 
                << "us, Avg=" << calc_avg(metrics_.read_latencies) << "us" << std::endl;
    }
    if (!metrics_.write_latencies.empty()) {
      std::cout << "  Write:  P50=" << calc_p50(metrics_.write_latencies) 
                << "us, Avg=" << calc_avg(metrics_.write_latencies) << "us" << std::endl;
    }
    if (!metrics_.range_latencies.empty()) {
      std::cout << "  Range:  P50=" << calc_p50(metrics_.range_latencies) 
                << "us, Avg=" << calc_avg(metrics_.range_latencies) << "us" << std::endl;
    }
    std::cout << std::endl;
  }
  
  int duration_sec_;
  int num_threads_;
  uint64_t num_keys_;
  std::string data_dir_;
  std::atomic<bool> stop_;
  
  std::shared_ptr<CedarGraphStorage> storage_;
  std::vector<std::string> keys_;
  PerformanceMetrics metrics_;
};

int main(int argc, char* argv[]) {
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     CedarGraph REAL System Performance Test                ║" << std::endl;
  std::cout << "║     (Using actual CedarGraphStorage)                       ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  int duration = 30;
  int threads = 8;
  uint64_t keys = 100000;
  std::string data_dir = "/tmp/cedar_real_benchmark";
  
  // 解析参数
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--duration" && i + 1 < argc) {
      duration = std::stoi(argv[++i]);
    } else if (arg == "--threads" && i + 1 < argc) {
      threads = std::stoi(argv[++i]);
    } else if (arg == "--keys" && i + 1 < argc) {
      keys = std::stoull(argv[++i]);
    } else if (arg == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
    }
  }
  
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Duration: " << duration << " seconds" << std::endl;
  std::cout << "  Threads:  " << threads << std::endl;
  std::cout << "  Keys:     " << keys << std::endl;
  std::cout << "  Data Dir: " << data_dir << std::endl;
  std::cout << std::endl;
  
  RealStorageBenchmark benchmark(duration, threads, keys, data_dir);
  
  Status s = benchmark.Initialize();
  if (!s.ok()) {
    std::cerr << "Initialize failed: " << s.ToString() << std::endl;
    return 1;
  }
  
  s = benchmark.PrepareData();
  if (!s.ok()) {
    std::cerr << "Prepare data failed: " << s.ToString() << std::endl;
    return 1;
  }
  
  benchmark.Run();
  
  std::cout << "\nBenchmark completed!" << std::endl;
  return 0;
}
