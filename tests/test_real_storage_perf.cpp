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
// See the License for the specific use governing permissions and
// limitations under the License.

// =============================================================================
// REAL CedarGraphStorage Performance Test
// 真实 CedarGraphStorage 性能测试 - 接入真实图存储 API
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
#include "cedar/types/cedar_types.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

// 性能指标
struct PerfMetrics {
  std::atomic<uint64_t> total_ops{0};
  std::atomic<uint64_t> vertex_reads{0};
  std::atomic<uint64_t> vertex_writes{0};
  std::atomic<uint64_t> edge_reads{0};
  std::atomic<uint64_t> edge_writes{0};
  std::atomic<uint64_t> failed_ops{0};
  
  std::vector<uint64_t> latencies;
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

// 真实存储性能测试
class RealStoragePerfTest {
 public:
  RealStoragePerfTest(int duration_sec, int num_threads, 
                      uint64_t num_vertices, uint64_t num_edges,
                      const std::string& data_dir)
      : duration_sec_(duration_sec), num_threads_(num_threads),
        num_vertices_(num_vertices), num_edges_(num_edges),
        data_dir_(data_dir), stop_(false) {}
  
  ~RealStoragePerfTest() {
    Cleanup();
  }
  
  Status Initialize() {
    std::cout << "[RealStorage] Initializing..." << std::endl;
    
    // 清理并创建数据目录
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);
    
    // 配置 CedarGraphStorage
    CedarOptions options;
    options.create_if_missing = true;
    options.write_buffer_size = 64 * 1024 * 1024;  // 64MB
    
    // 打开存储
    CedarGraphStorage* storage = nullptr;
    Status s = CedarGraphStorage::Open(options, data_dir_, &storage);
    if (!s.ok()) {
      std::cerr << "Failed to open storage: " << s.ToString() << std::endl;
      return s;
    }
    storage_.reset(storage);
    
    std::cout << "[RealStorage] Storage opened at: " << data_dir_ << std::endl;
    
    // 预生成顶点 ID
    vertex_ids_.reserve(num_vertices_);
    for (uint64_t i = 0; i < num_vertices_; ++i) {
      vertex_ids_.push_back(i);
    }
    
    // 预生成边
    edge_pairs_.reserve(num_edges_);
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint64_t> dist(0, num_vertices_ - 1);
    for (uint64_t i = 0; i < num_edges_; ++i) {
      uint64_t src = dist(rng);
      uint64_t dst = dist(rng);
      if (src != dst) {
        edge_pairs_.push_back({src, dst});
      }
    }
    
    std::cout << "[RealStorage] Generated " << vertex_ids_.size() 
              << " vertices, " << edge_pairs_.size() << " edges" << std::endl;
    
    return Status::OK();
  }
  
  Status PrepareData() {
    std::cout << "[RealStorage] Preloading data..." << std::endl;
    
    // 批量写入顶点
    const size_t BATCH = 10000;
    size_t written = 0;
    Timestamp txn_version{1};
    
    for (size_t i = 0; i < vertex_ids_.size(); i += BATCH) {
      size_t end = std::min(i + BATCH, vertex_ids_.size());
      
      for (size_t j = i; j < end; ++j) {
        auto vertex = CreateVertex(vertex_ids_[j]);
        
        // 使用 CedarGraphStorage API 写入顶点
        // entity_id = vertex_id, tx_time = 1 (static data)
        Status s = storage_->Put(vertex_ids_[j], 1, vertex, txn_version);
        if (!s.ok()) {
          return s;
        }
        
        written++;
      }
      
      if ((i / BATCH) % 10 == 0) {
        std::cout << "  Loaded " << written << " vertices..." << std::endl;
      }
    }
    
    // 写入边
    std::cout << "[RealStorage] Loading edges..." << std::endl;
    Timestamp edge_version{2};
    for (size_t i = 0; i < std::min(edge_pairs_.size(), (size_t)100000); ++i) {
      auto [src, dst] = edge_pairs_[i];
      auto edge = CreateEdge(i);
      
      // 使用 PutEdge API
      Status s = storage_->PutEdge(src, dst, 1, 1, edge, edge_version);
      if (!s.ok()) {
        // 边写入失败不致命，继续
        continue;
      }
    }
    
    std::cout << "[RealStorage] Data preload complete!" << std::endl;
    return Status::OK();
  }
  
  void Run() {
    std::cout << "\n[RealStorage] Running benchmark for " << duration_sec_ 
              << " seconds with " << num_threads_ << " threads..." << std::endl;
    
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
    std::uniform_int_distribution<size_t> vertex_dist(0, vertex_ids_.size() - 1);
    std::uniform_int_distribution<size_t> edge_dist(0, edge_pairs_.size() - 1);
    std::uniform_int_distribution<int> op_dist(0, 99);
    
    Timestamp txn_version{3};
    Timestamp read_version{2};
    
    while (!stop_) {
      int op = op_dist(rng);
      
      if (op < 40) {
        // 40% 顶点读
        ReadVertex(vertex_dist(rng), read_version);
      } else if (op < 60) {
        // 20% 顶点写
        WriteVertex(vertex_dist(rng), txn_version);
      } else if (op < 75) {
        // 15% 边读
        ReadEdge(edge_dist(rng), read_version);
      } else if (op < 85) {
        // 10% 边写
        WriteEdge(edge_dist(rng), txn_version);
      } else {
        // 15% 扫描
        DoScan();
      }
    }
  }
  
  void ReadVertex(size_t idx, Timestamp read_version) {
    auto start = std::chrono::high_resolution_clock::now();
    
    uint64_t vertex_id = vertex_ids_[idx];
    
    // 使用 CedarGraphStorage::Get API
    auto result = storage_->Get(vertex_id, 1);  // entity_id, tx_time
    
    auto end = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    RecordLatency(latency);
    metrics_.vertex_reads.fetch_add(1);
    metrics_.total_ops.fetch_add(1);
  }
  
  void WriteVertex(size_t idx, Timestamp txn_version) {
    auto start = std::chrono::high_resolution_clock::now();
    
    uint64_t vertex_id = vertex_ids_[idx];
    auto vertex = CreateVertex(vertex_id);
    
    // 使用 CedarGraphStorage::Put API
    Status s = storage_->Put(vertex_id, 1, vertex, txn_version);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    RecordLatency(latency);
    
    if (s.ok()) {
      metrics_.vertex_writes.fetch_add(1);
      metrics_.total_ops.fetch_add(1);
    } else {
      metrics_.failed_ops.fetch_add(1);
    }
  }
  
  void ReadEdge(size_t idx, Timestamp read_version) {
    auto start = std::chrono::high_resolution_clock::now();
    
    auto [src, dst] = edge_pairs_[idx];
    
    // 使用 CedarGraphStorage::GetEdge API
    auto result = storage_->GetEdge(src, dst, 1, read_version);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    RecordLatency(latency);
    metrics_.edge_reads.fetch_add(1);
    metrics_.total_ops.fetch_add(1);
  }
  
  void WriteEdge(size_t idx, Timestamp txn_version) {
    auto start = std::chrono::high_resolution_clock::now();
    
    auto [src, dst] = edge_pairs_[idx];
    auto edge = CreateEdge(idx);
    
    // 使用 CedarGraphStorage::PutEdge API
    Status s = storage_->PutEdge(src, dst, 1, 1, edge, txn_version);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    RecordLatency(latency);
    
    if (s.ok()) {
      metrics_.edge_writes.fetch_add(1);
      metrics_.total_ops.fetch_add(1);
    } else {
      metrics_.failed_ops.fetch_add(1);
    }
  }
  
  void DoScan() {
    auto start = std::chrono::high_resolution_clock::now();
    
    // 使用 ScanEdges API - 扫描第一个顶点的出边
    if (!vertex_ids_.empty()) {
      Timestamp start_ts{0};
      Timestamp end_ts{100};
      auto results = storage_->ScanEdges(vertex_ids_[0], 0, start_ts, end_ts);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    RecordLatency(latency);
    metrics_.total_ops.fetch_add(1);
  }
  
  void RecordLatency(uint64_t latency) {
    std::lock_guard<std::mutex> lock(metrics_.latency_mutex);
    metrics_.latencies.push_back(latency);
  }
  
  Descriptor CreateVertex(uint64_t id) {
    // Descriptor 是一个紧凑的 64-bit 结构
    // 创建 InlineInt 类型的 Descriptor
    Descriptor desc(cedar::EntryKind::InlineInt, 0, 
                    static_cast<uint32_t>(id & 0xFFFFFFFF), 8);
    return desc;
  }
  
  Descriptor CreateEdge(uint64_t id) {
    // 创建 EdgeRef 类型的 Descriptor 表示边
    Descriptor desc(cedar::EntryKind::EdgeRef, 1, 
                    static_cast<uint32_t>(id & 0xFFFFFFFF), 8);
    return desc;
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
    std::cout << "  Total:        " << metrics_.total_ops.load() << std::endl;
    std::cout << "  Vertex Reads: " << metrics_.vertex_reads.load() << std::endl;
    std::cout << "  Vertex Writes:" << metrics_.vertex_writes.load() << std::endl;
    std::cout << "  Edge Reads:   " << metrics_.edge_reads.load() << std::endl;
    std::cout << "  Edge Writes:  " << metrics_.edge_writes.load() << std::endl;
    std::cout << "  Failed:       " << metrics_.failed_ops.load() << std::endl;
    std::cout << std::endl;
    
    std::cout << "Throughput: " << std::fixed << std::setprecision(1) 
              << metrics_.GetThroughput() << " ops/sec" << std::endl;
    std::cout << std::endl;
    
    // 延迟统计
    if (!metrics_.latencies.empty()) {
      auto sorted = metrics_.latencies;
      std::sort(sorted.begin(), sorted.end());
      
      auto p50 = sorted[sorted.size() * 0.5];
      auto p99 = sorted[std::min(sorted.size() - 1, (size_t)(sorted.size() * 0.99))];
      auto avg = std::accumulate(sorted.begin(), sorted.end(), 0.0) / sorted.size();
      
      std::cout << "Latency (microseconds):" << std::endl;
      std::cout << "  P50: " << p50 << " µs" << std::endl;
      std::cout << "  P99: " << p99 << " µs" << std::endl;
      std::cout << "  Avg: " << std::fixed << std::setprecision(1) << avg << " µs" << std::endl;
    }
    std::cout << std::endl;
  }
  
  int duration_sec_;
  int num_threads_;
  uint64_t num_vertices_;
  uint64_t num_edges_;
  std::string data_dir_;
  std::atomic<bool> stop_;
  
  std::shared_ptr<CedarGraphStorage> storage_;
  std::vector<uint64_t> vertex_ids_;
  std::vector<std::pair<uint64_t, uint64_t>> edge_pairs_;
  PerfMetrics metrics_;
};

int main(int argc, char* argv[]) {
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     CedarGraph REAL Storage Performance Test               ║" << std::endl;
  std::cout << "║     (Using actual CedarGraphStorage API)                   ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  int duration = 30;
  int threads = 8;
  uint64_t vertices = 10000;
  uint64_t edges = 50000;
  std::string data_dir = "/tmp/cedar_real_storage_perf";
  
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--duration" && i + 1 < argc) {
      duration = std::stoi(argv[++i]);
    } else if (arg == "--threads" && i + 1 < argc) {
      threads = std::stoi(argv[++i]);
    } else if (arg == "--vertices" && i + 1 < argc) {
      vertices = std::stoull(argv[++i]);
    } else if (arg == "--edges" && i + 1 < argc) {
      edges = std::stoull(argv[++i]);
    } else if (arg == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
    }
  }
  
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Duration: " << duration << " seconds" << std::endl;
  std::cout << "  Threads:  " << threads << std::endl;
  std::cout << "  Vertices: " << vertices << std::endl;
  std::cout << "  Edges:    " << edges << std::endl;
  std::cout << "  Data Dir: " << data_dir << std::endl;
  std::cout << std::endl;
  
  RealStoragePerfTest test(duration, threads, vertices, edges, data_dir);
  
  Status s = test.Initialize();
  if (!s.ok()) {
    std::cerr << "Initialize failed: " << s.ToString() << std::endl;
    return 1;
  }
  
  s = test.PrepareData();
  if (!s.ok()) {
    std::cerr << "Prepare data failed: " << s.ToString() << std::endl;
    return 1;
  }
  
  test.Run();
  
  std::cout << "\nBenchmark completed!" << std::endl;
  return 0;
}
