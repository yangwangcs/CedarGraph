// Copyright 2025 The Cedar Authors
// Distributed Performance Test for CedarGraph

#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <thread>
#include <atomic>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <cmath>

#include <grpcpp/grpcpp.h>
#include "storage_service.pb.h"
#include "storage_service.grpc.pb.h"
#include "meta_service.pb.h"
#include "meta_service.grpc.pb.h"

// Cedar headers
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace std::chrono;

// =============================================================================
// Test Configuration
// =============================================================================
struct TestConfig {
  std::vector<std::string> storage_endpoints = {
    "127.0.0.1:7001",
    "127.0.0.1:7002",
    "127.0.0.1:7003"
  };
  std::vector<std::string> metad_endpoints = {
    "127.0.0.1:2379",
    "127.0.0.1:2380",
    "127.0.0.1:2381"
  };
  
  // Test scale
  int num_vertices = 10000;
  int num_edges = 50000;
  int num_properties = 20000;
  int num_timestamps = 100;  // Versions per entity
  
  // Test parameters
  int warmup_iterations = 1000;
  int test_iterations = 10000;
  int batch_size = 100;
  int num_threads = 4;
};

// =============================================================================
// Storage Client
// =============================================================================
class StorageTestClient {
 public:
  StorageTestClient(const std::string& endpoint) {
    channel_ = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    stub_ = cedar::storage::StorageService::NewStub(channel_);
  }
  
  // Write a single vertex property
  bool WriteVertex(uint64_t vertex_id, uint16_t col_id, int32_t value, 
                   uint64_t timestamp, uint64_t txn_id = 0) {
    cedar::storage::PutRequest request;
    auto* key = request.mutable_key();
    key->set_entity_id(vertex_id);
    key->set_timestamp(timestamp);
    key->set_column_id(col_id);
    key->set_type_flags((0 << 16));  // Vertex type
    key->set_partition_id(0);
    
    auto* desc = request.mutable_descriptor_();
    desc->set_data(reinterpret_cast<const char*>(&value), sizeof(value));
    
    request.mutable_txn_version()->set_value(timestamp);
    request.set_txn_id(txn_id);
    
    cedar::storage::PutResponse response;
    grpc::ClientContext context;
    auto status = stub_->Put(&context, request, &response);
    
    return status.ok() && response.success();
  }
  
  // Read a vertex property
  std::optional<int32_t> ReadVertex(uint64_t vertex_id, uint16_t col_id, uint64_t timestamp) {
    cedar::storage::GetRequest request;
    auto* key = request.mutable_key();
    key->set_entity_id(vertex_id);
    key->set_timestamp(timestamp);
    key->set_column_id(col_id);
    key->set_type_flags((0 << 16));
    key->set_partition_id(0);
    
    cedar::storage::GetResponse response;
    grpc::ClientContext context;
    auto status = stub_->Get(&context, request, &response);
    
    if (status.ok() && response.success() && response.found() && 
        response.has_descriptor_() && response.descriptor_().data().size() >= sizeof(int32_t)) {
      return *reinterpret_cast<const int32_t*>(response.descriptor_().data().data());
    }
    return std::nullopt;
  }
  
  // Batch write
  bool BatchWrite(const std::vector<std::tuple<uint64_t, uint16_t, int32_t, uint64_t>>& items,
                  uint64_t txn_id = 0) {
    cedar::storage::BatchPutRequest request;
    
    for (const auto& [entity_id, col_id, value, timestamp] : items) {
      auto* item = request.add_items();
      auto* key = item->mutable_key();
      key->set_entity_id(entity_id);
      key->set_timestamp(timestamp);
      key->set_column_id(col_id);
      key->set_type_flags((0 << 16));
      key->set_partition_id(0);
      
      auto* desc = item->mutable_descriptor_();
      desc->set_data(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    
    request.mutable_txn_version()->set_value(items.empty() ? 0 : std::get<3>(items[0]));
    request.set_txn_id(txn_id);
    
    cedar::storage::BatchPutResponse response;
    grpc::ClientContext context;
    auto status = stub_->BatchPut(&context, request, &response);
    
    return status.ok() && response.success();
  }
  
  // Batch read
  std::vector<std::optional<int32_t>> BatchRead(
      const std::vector<std::tuple<uint64_t, uint16_t, uint64_t>>& keys) {
    cedar::storage::BatchGetRequest request;
    
    for (const auto& [entity_id, col_id, timestamp] : keys) {
      auto* key = request.add_keys();
      key->set_entity_id(entity_id);
      key->set_timestamp(timestamp);
      key->set_column_id(col_id);
      key->set_type_flags((0 << 16));
      key->set_partition_id(0);
    }
    
    cedar::storage::BatchGetResponse response;
    grpc::ClientContext context;
    auto status = stub_->BatchGet(&context, request, &response);
    
    std::vector<std::optional<int32_t>> results;
    if (status.ok() && response.success()) {
      for (int i = 0; i < response.found_size(); ++i) {
        if (response.found(i) && response.descriptors(i).data().size() >= sizeof(int32_t)) {
          results.push_back(*reinterpret_cast<const int32_t*>(
              response.descriptors(i).data().data()));
        } else {
          results.push_back(std::nullopt);
        }
      }
    }
    return results;
  }
  
  // 2PC Prepare
  bool Prepare(uint64_t txn_id, const std::vector<uint64_t>& read_set,
               const std::vector<uint64_t>& write_set, uint64_t commit_ts) {
    cedar::storage::PrepareRequest request;
    request.set_txn_id(txn_id);
    request.set_commit_ts(commit_ts);
    
    for (auto id : read_set) {
      auto* key = request.add_read_set();
      key->set_entity_id(id);
      key->set_partition_id(0);
    }
    for (auto id : write_set) {
      auto* key = request.add_write_set();
      key->set_entity_id(id);
      key->set_partition_id(0);
    }
    
    cedar::storage::PrepareResponse response;
    grpc::ClientContext context;
    auto status = stub_->Prepare(&context, request, &response);
    
    return status.ok() && response.prepared();
  }
  
  // 2PC Commit
  bool Commit(uint64_t txn_id, uint64_t commit_ts) {
    cedar::storage::CommitRequest request;
    request.set_txn_id(txn_id);
    request.set_commit_ts(commit_ts);
    
    cedar::storage::CommitResponse response;
    grpc::ClientContext context;
    auto status = stub_->Commit(&context, request, &response);
    
    return status.ok() && response.success();
  }
  
 private:
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<cedar::storage::StorageService::Stub> stub_;
};

// =============================================================================
// Performance Test Suite
// =============================================================================
class PerformanceTestSuite {
 public:
  PerformanceTestSuite(const TestConfig& config) : config_(config) {
    // Create clients for each storage node
    for (const auto& endpoint : config_.storage_endpoints) {
      clients_.emplace_back(endpoint);
    }
    
    // Random generator
    rng_.seed(42);
  }
  
  // Run all tests
  void RunAllTests() {
    std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║     CedarGraph Distributed Performance Test Suite          ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    
    std::cout << "Test Configuration:" << std::endl;
    std::cout << "  Vertices:    " << config_.num_vertices << std::endl;
    std::cout << "  Edges:       " << config_.num_edges << std::endl;
    std::cout << "  Properties:  " << config_.num_properties << std::endl;
    std::cout << "  Timestamps:  " << config_.num_timestamps << " (versions per entity)" << std::endl;
    std::cout << "  Threads:     " << config_.num_threads << std::endl;
    std::cout << "  Batch Size:  " << config_.batch_size << std::endl;
    std::cout << std::endl;
    
    // Warmup
    std::cout << "[Warming up...]" << std::endl;
    Warmup();
    
    // Run tests
    TestTemporalWriteThroughput();
    TestTemporalReadThroughput();
    TestTemporalPointQuery();
    TestTemporalRangeQuery();
    TestGraphAnalysis();
    Test2PCPerformance();
    TestSpaceUsage();
    
    // Print summary
    PrintSummary();
  }
  
 private:
  void Warmup() {
    for (int i = 0; i < config_.warmup_iterations; ++i) {
      uint64_t vid = rng_() % config_.num_vertices;
      int32_t value = rng_();
      clients_[0].WriteVertex(vid, 1, value, 1000000 + i);
    }
  }
  
  // Test 1: Temporal Write Throughput
  void TestTemporalWriteThroughput() {
    std::cout << "[Test 1] Temporal Write Throughput" << std::endl;
    
    const int num_writes = config_.test_iterations;
    std::atomic<int> success_count{0};
    
    auto start = high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    int writes_per_thread = num_writes / config_.num_threads;
    
    for (int t = 0; t < config_.num_threads; ++t) {
      threads.emplace_back([&, t]() {
        int client_idx = t % clients_.size();
        for (int i = 0; i < writes_per_thread; ++i) {
          uint64_t vid = (t * writes_per_thread + i) % config_.num_vertices;
          int32_t value = rng_();
          uint64_t ts = 1000000 + i;
          
          if (clients_[client_idx].WriteVertex(vid, 1, value, ts)) {
            success_count++;
          }
        }
      });
    }
    
    for (auto& t : threads) t.join();
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start).count();
    double throughput = (double)success_count / (duration / 1000000.0);
    double latency = (double)duration / success_count;
    
    results_["Write Throughput"] = {throughput, latency, success_count};
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << throughput 
              << " ops/sec" << std::endl;
    std::cout << "  Latency:    " << std::setprecision(2) << latency << " μs/op" << std::endl;
    std::cout << "  Success:    " << success_count << "/" << num_writes << std::endl;
    std::cout << std::endl;
  }
  
  // Test 2: Temporal Read Throughput
  void TestTemporalReadThroughput() {
    std::cout << "[Test 2] Temporal Read Throughput" << std::endl;
    
    // First write some data
    std::cout << "  Preparing data..." << std::endl;
    for (int i = 0; i < 10000; ++i) {
      uint64_t vid = i % config_.num_vertices;
      clients_[0].WriteVertex(vid, 1, i * 100, 1000000 + i);
    }
    
    const int num_reads = config_.test_iterations;
    std::atomic<int> success_count{0};
    
    auto start = high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    int reads_per_thread = num_reads / config_.num_threads;
    
    for (int t = 0; t < config_.num_threads; ++t) {
      threads.emplace_back([&, t]() {
        int client_idx = t % clients_.size();
        for (int i = 0; i < reads_per_thread; ++i) {
          uint64_t vid = rng_() % 10000;
          uint64_t ts = 1000000 + (rng_() % 10000);
          
          auto result = clients_[client_idx].ReadVertex(vid, 1, ts);
          if (result.has_value()) {
            success_count++;
          }
        }
      });
    }
    
    for (auto& t : threads) t.join();
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start).count();
    double throughput = (double)success_count / (duration / 1000000.0);
    double latency = (double)duration / success_count;
    
    results_["Read Throughput"] = {throughput, latency, success_count};
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << throughput 
              << " ops/sec" << std::endl;
    std::cout << "  Latency:    " << std::setprecision(2) << latency << " μs/op" << std::endl;
    std::cout << "  Success:    " << success_count << "/" << num_reads << std::endl;
    std::cout << std::endl;
  }
  
  // Test 3: Temporal Point Query (exact timestamp)
  void TestTemporalPointQuery() {
    std::cout << "[Test 3] Temporal Point Query" << std::endl;
    
    // Write multiple versions
    std::cout << "  Writing " << config_.num_timestamps << " versions..." << std::endl;
    uint64_t vid = 12345;
    for (int i = 0; i < config_.num_timestamps; ++i) {
      clients_[0].WriteVertex(vid, 1, i * 10, 1000000 + i * 1000);
    }
    
    const int num_queries = 50000;
    std::atomic<int> success_count{0};
    
    auto start = high_resolution_clock::now();
    
    for (int i = 0; i < num_queries; ++i) {
      uint64_t ts = 1000000 + (rng_() % config_.num_timestamps) * 1000;
      auto result = clients_[0].ReadVertex(vid, 1, ts);
      if (result.has_value()) {
        success_count++;
      }
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start).count();
    double throughput = (double)num_queries / (duration / 1000000.0);
    double latency = (double)duration / num_queries;
    
    results_["Point Query"] = {throughput, latency, success_count};
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << throughput 
              << " ops/sec" << std::endl;
    std::cout << "  Latency:    " << std::setprecision(2) << latency << " μs/op" << std::endl;
    std::cout << "  Success:    " << success_count << "/" << num_queries << std::endl;
    std::cout << std::endl;
  }
  
  // Test 4: Temporal Range Query
  void TestTemporalRangeQuery() {
    std::cout << "[Test 4] Temporal Range Query" << std::endl;
    
    // Range scan simulation (multiple point queries)
    const int num_ranges = 1000;
    const int range_size = 50;
    std::atomic<int> total_queries{0};
    std::atomic<int> success_count{0};
    
    auto start = high_resolution_clock::now();
    
    for (int r = 0; r < num_ranges; ++r) {
      uint64_t start_ts = 1000000 + (rng_() % (config_.num_timestamps - range_size)) * 1000;
      
      for (int i = 0; i < range_size; ++i) {
        uint64_t vid = 12345;
        uint64_t ts = start_ts + i * 1000;
        
        auto result = clients_[0].ReadVertex(vid, 1, ts);
        total_queries++;
        if (result.has_value()) {
          success_count++;
        }
      }
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start).count();
    double throughput = (double)total_queries / (duration / 1000000.0);
    double latency = (double)duration / total_queries;
    
    results_["Range Query"] = {throughput, latency, success_count};
    std::cout << "  Ranges:     " << num_ranges << std::endl;
    std::cout << "  Range size: " << range_size << std::endl;
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << throughput 
              << " ops/sec" << std::endl;
    std::cout << "  Latency:    " << std::setprecision(2) << latency << " μs/op" << std::endl;
    std::cout << std::endl;
  }
  
  // Test 5: Graph Analysis (simulated neighbor queries)
  void TestGraphAnalysis() {
    std::cout << "[Test 5] Graph Analysis (Neighbor Queries)" << std::endl;
    
    // Write some edge data (simulating adjacency list)
    std::cout << "  Preparing graph data..." << std::endl;
    const int num_vertices = 1000;
    const int edges_per_vertex = 10;
    
    for (int v = 0; v < num_vertices; ++v) {
      for (int e = 0; e < edges_per_vertex; ++e) {
        uint64_t dst = (v + e + 1) % num_vertices;
        clients_[0].WriteVertex(v, 100 + e, dst, 1000000);
      }
    }
    
    // Neighbor queries
    const int num_queries = 10000;
    std::atomic<int> success_count{0};
    std::atomic<int> total_neighbors{0};
    
    auto start = high_resolution_clock::now();
    
    for (int i = 0; i < num_queries; ++i) {
      uint64_t vid = rng_() % num_vertices;
      int found = 0;
      
      for (int e = 0; e < edges_per_vertex; ++e) {
        auto result = clients_[0].ReadVertex(vid, 100 + e, 1000000);
        if (result.has_value()) {
          found++;
        }
      }
      
      if (found > 0) {
        success_count++;
        total_neighbors += found;
      }
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start).count();
    double throughput = (double)num_queries / (duration / 1000000.0);
    double avg_neighbors = (double)total_neighbors / success_count;
    
    results_["Graph Analysis"] = {throughput, (double)duration / num_queries, success_count};
    std::cout << "  Throughput:     " << std::fixed << std::setprecision(2) << throughput 
              << " queries/sec" << std::endl;
    std::cout << "  Latency:        " << std::setprecision(2) << (double)duration / num_queries 
              << " μs/query" << std::endl;
    std::cout << "  Avg neighbors:  " << std::setprecision(2) << avg_neighbors << std::endl;
    std::cout << std::endl;
  }
  
  // Test 6: 2PC Transaction Performance
  void Test2PCPerformance() {
    std::cout << "[Test 6] 2PC Transaction Performance" << std::endl;
    
    const int num_txns = 1000;
    std::atomic<int> success_count{0};
    std::atomic<int> prepared_count{0};
    
    auto start = high_resolution_clock::now();
    
    for (int t = 0; t < num_txns; ++t) {
      uint64_t txn_id = 1000 + t;
      std::vector<uint64_t> read_set;
      std::vector<uint64_t> write_set;
      
      // Generate random read/write sets
      for (int i = 0; i < 5; ++i) {
        read_set.push_back(rng_() % 1000);
        write_set.push_back(rng_() % 1000);
      }
      
      uint64_t commit_ts = 1000000 + t;
      
      // Phase 1: Prepare
      if (clients_[0].Prepare(txn_id, read_set, write_set, commit_ts)) {
        prepared_count++;
        
        // Phase 2: Commit
        if (clients_[0].Commit(txn_id, commit_ts)) {
          success_count++;
        }
      }
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start).count();
    double throughput = (double)success_count / (duration / 1000000.0);
    double latency = (double)duration / num_txns;
    
    results_["2PC Transaction"] = {throughput, latency, success_count};
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << throughput 
              << " txns/sec" << std::endl;
    std::cout << "  Latency:    " << std::setprecision(2) << latency << " μs/txn" << std::endl;
    std::cout << "  Prepared:   " << prepared_count << "/" << num_txns << std::endl;
    std::cout << "  Committed:  " << success_count << "/" << num_txns << std::endl;
    std::cout << std::endl;
  }
  
  // Test 7: Space Usage
  void TestSpaceUsage() {
    std::cout << "[Test 7] Space Usage Analysis" << std::endl;
    
    // Get partition info from storage nodes
    for (size_t i = 0; i < clients_.size(); ++i) {
      cedar::storage::GetPartitionInfoRequest request;
      request.set_partition_id(0);
      
      cedar::storage::GetPartitionInfoResponse response;
      grpc::ClientContext context;
      
      auto stub = cedar::storage::StorageService::NewStub(
          grpc::CreateChannel(config_.storage_endpoints[i], grpc::InsecureChannelCredentials()));
      auto status = stub->GetPartitionInfo(&context, request, &response);
      
      if (status.ok() && response.success()) {
        std::cout << "  Storage Node " << (i + 1) << ":" << std::endl;
        std::cout << "    Data size:   " << FormatBytes(response.info().data_size()) << std::endl;
        std::cout << "    Key count:   " << response.info().key_count() << std::endl;
        std::cout << "    Is leader:   " << (response.info().is_leader() ? "Yes" : "No") << std::endl;
      }
    }
    
    // Check data directory sizes
    std::cout << "  Data Directory Sizes:" << std::endl;
    for (int i = 1; i <= 3; ++i) {
      std::string cmd = "du -sh /tmp/cedar/storage/node" + std::to_string(i) + " 2>/dev/null | cut -f1";
      FILE* pipe = popen(cmd.c_str(), "r");
      if (pipe) {
        char buffer[128];
        if (fgets(buffer, sizeof(buffer), pipe)) {
          std::cout << "    node" << i << ": " << buffer;
        }
        pclose(pipe);
      }
    }
    std::cout << std::endl;
  }
  
  void PrintSummary() {
    std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║                    Performance Summary                     ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    
    std::cout << std::left << std::setw(25) << "Test" 
              << std::right << std::setw(15) << "Throughput"
              << std::setw(15) << "Latency"
              << std::setw(12) << "Success" << std::endl;
    std::cout << std::string(67, '-') << std::endl;
    
    for (const auto& [name, result] : results_) {
      std::cout << std::left << std::setw(25) << name
                << std::right << std::setw(14) << std::fixed << std::setprecision(2) << result.throughput << " "
                << std::setw(13) << std::setprecision(2) << result.latency << " μs"
                << std::setw(11) << result.success_count << std::endl;
    }
    
    std::cout << std::endl;
    std::cout << "Legend:" << std::endl;
    std::cout << "  Throughput: Operations per second" << std::endl;
    std::cout << "  Latency:    Average time per operation (microseconds)" << std::endl;
    std::cout << std::endl;
  }
  
  std::string FormatBytes(uint64_t bytes) {
    const char* units[] = {"B", "KB", "MB", "GB"};
    int unit = 0;
    double size = bytes;
    while (size >= 1024 && unit < 3) {
      size /= 1024;
      unit++;
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << size << " " << units[unit];
    return oss.str();
  }
  
  struct TestResult {
    double throughput;
    double latency;
    int success_count;
  };
  
  TestConfig config_;
  std::vector<StorageTestClient> clients_;
  std::mt19937 rng_;
  std::unordered_map<std::string, TestResult> results_;
};

// =============================================================================
// Main Entry Point
// =============================================================================
int main(int argc, char* argv[]) {
  TestConfig config;
  
  // Parse command line arguments
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--vertices" && i + 1 < argc) {
      config.num_vertices = std::stoi(argv[++i]);
    } else if (arg == "--edges" && i + 1 < argc) {
      config.num_edges = std::stoi(argv[++i]);
    } else if (arg == "--iterations" && i + 1 < argc) {
      config.test_iterations = std::stoi(argv[++i]);
    } else if (arg == "--threads" && i + 1 < argc) {
      config.num_threads = std::stoi(argv[++i]);
    } else if (arg == "--help") {
      std::cout << "CedarGraph Distributed Performance Test" << std::endl;
      std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
      std::cout << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  --vertices N      Number of vertices (default: 10000)" << std::endl;
      std::cout << "  --edges N         Number of edges (default: 50000)" << std::endl;
      std::cout << "  --iterations N    Test iterations (default: 10000)" << std::endl;
      std::cout << "  --threads N       Number of threads (default: 4)" << std::endl;
      std::cout << std::endl;
      return 0;
    }
  }
  
  try {
    PerformanceTestSuite suite(config);
    suite.RunAllTests();
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
  
  return 0;
}
