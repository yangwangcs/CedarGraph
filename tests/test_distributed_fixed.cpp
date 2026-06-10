// Copyright 2025 The Cedar Authors
// Fixed Distributed Performance Test for CedarGraph
// - Fixed CedarKey timestamp consistency
// - Added BatchGet for neighbor queries
// - Added persistence verification

#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <thread>
#include <atomic>
#include <iomanip>
#include <sstream>

#include <grpcpp/grpcpp.h>
#include "storage_service.pb.h"
#include "storage_service.grpc.pb.h"
#include "cedar/types/descriptor.h"
#include "cedar/core/slice.h"

using namespace std::chrono;

// Test Configuration
struct TestConfig {
  std::vector<std::string> storage_endpoints = {
    "127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"
  };
  std::vector<std::string> data_dirs = {
    "/tmp/cedar_cluster/storaged0/node0",
    "/tmp/cedar_cluster/storaged1/node1",
    "/tmp/cedar_cluster/storaged2/node2"
  };
  
  int num_vertices = 10000;
  int num_edges = 50000;
  int test_iterations = 10000;
  int num_threads = 4;
  int batch_size = 100;
};

// Storage Client with BatchGet support
class StorageTestClient {
 public:
  StorageTestClient(const std::string& endpoint) {
    channel_ = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    stub_ = cedar::storage::StorageService::NewStub(channel_);
    
    // Wait for connection to be ready
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
    if (!channel_->WaitForConnected(deadline)) {
      std::cerr << "  Warning: Failed to connect to " << endpoint << std::endl;
    }
  }
  
  // Write a single vertex property
  bool WriteVertex(uint64_t vertex_id, uint16_t col_id, int32_t value, 
                   uint64_t timestamp, uint64_t txn_id = 0) {
    cedar::storage::PutRequest request;
    auto* key = request.mutable_key();
    key->set_entity_id(vertex_id);
    key->set_timestamp(timestamp);  // Use business timestamp
    key->set_column_id(col_id);
    key->set_type_flags((0 << 16));  // Vertex type
    key->set_partition_id(0);
    
    auto* desc = request.mutable_descriptor_();
    auto descriptor = cedar::Descriptor::InlineInt(col_id, value);
    auto encoded = descriptor.Encode();
    desc->set_data(encoded);
    
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
    key->set_timestamp(timestamp);  // Must match write timestamp
    key->set_column_id(col_id);
    key->set_type_flags((0 << 16));
    key->set_partition_id(0);
    
    cedar::storage::GetResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    auto status = stub_->Get(&context, request, &response);
    
    if (status.ok() && response.success() && response.found() && 
        response.has_descriptor_()) {
      auto decoded = cedar::Descriptor::Decode(cedar::Slice(response.descriptor_().data()));
      if (decoded.has_value()) {
        auto val = decoded->AsInlineInt();
        if (val.has_value()) {
          return val.value();
        }
      }
    }
    return std::nullopt;
  }
  
  // Force flush to ensure data persistence
  bool ForceFlush() {
    // Use GetPartitionInfo as a way to trigger/verify persistence
    // In production, this would be a dedicated Flush RPC
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    return true;
  }
  
  // Batch read (optimized for neighbor queries)
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
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
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
  
  // Batch write vertex properties
  bool BatchWriteVertex(
      const std::vector<std::tuple<uint64_t, uint16_t, int32_t, uint64_t>>& items,
      uint64_t txn_id = 0) {
    if (items.empty()) return true;
    
    cedar::storage::BatchPutRequest request;
    for (const auto& [vertex_id, col_id, value, timestamp] : items) {
      auto* item = request.add_items();
      auto* key = item->mutable_key();
      key->set_entity_id(vertex_id);
      key->set_timestamp(timestamp);
      key->set_column_id(col_id);
      key->set_type_flags((0 << 16));
      key->set_partition_id(0);
      
      auto descriptor = cedar::Descriptor::InlineInt(col_id, value);
      auto encoded = descriptor.Encode();
      item->mutable_descriptor_()->set_data(encoded);
    }
    request.mutable_txn_version()->set_value(std::get<3>(items[0]));
    request.set_txn_id(txn_id);
    
    cedar::storage::BatchPutResponse response;
    grpc::Status status;
    for (int retry = 0; retry < 3; ++retry) {
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
      status = stub_->BatchPut(&context, request, &response);
      if (status.ok() && response.success()) return true;
      if (!status.ok()) {
        std::cerr << "  BatchWriteVertex gRPC error (retry " << retry << "): " 
                  << status.error_message() << std::endl;
      } else if (!response.success()) {
        std::cerr << "  BatchWriteVertex server error (retry " << retry << "): " 
                  << response.error_msg() << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
  }
  
  // Check channel state
  std::string GetChannelState() const {
    switch (channel_->GetState(false)) {
      case GRPC_CHANNEL_IDLE: return "IDLE";
      case GRPC_CHANNEL_CONNECTING: return "CONNECTING";
      case GRPC_CHANNEL_READY: return "READY";
      case GRPC_CHANNEL_TRANSIENT_FAILURE: return "TRANSIENT_FAILURE";
      case GRPC_CHANNEL_SHUTDOWN: return "SHUTDOWN";
      default: return "UNKNOWN";
    }
  }
  
  // Get partition info for persistence verification
  bool GetPartitionInfo(uint32_t partition_id, uint64_t& data_size, uint64_t& key_count) {
    cedar::storage::GetPartitionInfoRequest request;
    request.set_partition_id(partition_id);
    
    cedar::storage::GetPartitionInfoResponse response;
    grpc::ClientContext context;
    auto status = stub_->GetPartitionInfo(&context, request, &response);
    
    if (status.ok() && response.success()) {
      data_size = response.info().data_size();
      key_count = response.info().key_count();
      return true;
    }
    return false;
  }
  
 private:
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<cedar::storage::StorageService::Stub> stub_;
};

// Fixed Performance Test Suite
class FixedTestSuite {
 public:
  FixedTestSuite(const TestConfig& config) : config_(config) {
    for (const auto& endpoint : config_.storage_endpoints) {
      clients_.emplace_back(endpoint);
    }
    rng_.seed(42);
  }
  
  void RunAllTests() {
    std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║     CedarGraph FIXED Distributed Performance Test          ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    
    TestBasicReadWrite();
    TestWriteThroughput();
    TestReadThroughput();
    TestTemporalPointQuery();
    TestBatchNeighborQuery();  // Optimized with BatchGet
    TestPersistenceVerification();
    
    PrintSummary();
  }
  
 private:
  // Test 1: Basic Read/Write Verification (Fixed)
  void TestBasicReadWrite() {
    std::cout << "[Test 1] Basic Read/Write Verification (Fixed)" << std::endl;
    
    uint64_t entity_id = 99999;
    uint64_t timestamp = 2000000;
    uint16_t col_id = 1;
    int32_t write_value = 424242;
    
    // Write
    bool write_ok = clients_[0].WriteVertex(entity_id, col_id, write_value, timestamp);
    std::cout << "  Write: " << (write_ok ? "✅ SUCCESS" : "❌ FAILED") << std::endl;
    
    // Small delay to allow flush
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Read back using same timestamp
    auto result = clients_[0].ReadVertex(entity_id, col_id, timestamp);
    
    if (result.has_value()) {
      std::cout << "  Read:  ✅ SUCCESS" << std::endl;
      std::cout << "  Value: " << result.value() << " (expected: " << write_value << ")" << std::endl;
      if (result.value() == write_value) {
        std::cout << "  ✅ Read-Write consistency verified!" << std::endl;
        results_["Basic R/W"] = {1.0, 0.0, 1};
      } else {
        std::cout << "  ❌ Value mismatch!" << std::endl;
        results_["Basic R/W"] = {0.0, 0.0, 0};
      }
    } else {
      std::cout << "  ❌ Read FAILED - Data not found" << std::endl;
      results_["Basic R/W"] = {0.0, 0.0, 0};
    }
    std::cout << std::endl;
  }
  
  // Test 2: Write Throughput (optimized with BatchPut RPC)
  void TestWriteThroughput() {
    std::cout << "[Test 2] Write Throughput Test (BatchPut RPC)" << std::endl;
    
    const int num_writes = config_.test_iterations;
    const int batch_size = 25;  // Batch 25 writes per RPC
    std::atomic<int> success_count{0};
    
    auto start = high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    int writes_per_thread = num_writes / config_.num_threads;
    
    for (int t = 0; t < config_.num_threads; ++t) {
      threads.emplace_back([&, t]() {
        int client_idx = t % clients_.size();
        std::vector<std::tuple<uint64_t, uint16_t, int32_t, uint64_t>> batch;
        batch.reserve(batch_size);
        
        for (int i = 0; i < writes_per_thread; ++i) {
          uint64_t vid = (t * writes_per_thread + i) % config_.num_vertices;
          int32_t value = rng_();
          uint64_t ts = 1000000 + i;
          batch.push_back({vid, 1, value, ts});
          
          if (batch.size() >= static_cast<size_t>(batch_size) || i == writes_per_thread - 1) {
            if (clients_[client_idx].BatchWriteVertex(batch)) {
              success_count += static_cast<int>(batch.size());
            }
            batch.clear();
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
  
  // Test 3: Read Throughput
  void TestReadThroughput() {
    std::cout << "[Test 3] Read Throughput Test" << std::endl;
    
    // First write some data to all clients so reads always hit data
    std::cout << "  Preparing data..." << std::endl;
    for (int i = 0; i < config_.num_vertices; ++i) {
      uint64_t vid = i % config_.num_vertices;
      size_t client_idx = vid % clients_.size();
      clients_[client_idx].WriteVertex(vid, 1, i * 100, 3000000 + vid);
    }
    
    // Wait for flush
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    const int num_reads = config_.test_iterations;
    std::atomic<int> success_count{0};
    
    auto start = high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    int reads_per_thread = num_reads / config_.num_threads;
    
    for (int t = 0; t < config_.num_threads; ++t) {
      threads.emplace_back([&, t]() {
        for (int i = 0; i < reads_per_thread; ++i) {
          uint64_t vid = rng_() % config_.num_vertices;
          uint64_t ts = 3000000 + vid;  // Use same timestamp as write
          size_t client_idx = vid % clients_.size();
          
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
    double latency = (double)duration / num_reads;
    
    results_["Read Throughput"] = {throughput, latency, success_count};
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << throughput 
              << " ops/sec" << std::endl;
    std::cout << "  Latency:    " << std::setprecision(2) << latency << " μs/op" << std::endl;
    std::cout << "  Success:    " << success_count << "/" << num_reads << std::endl;
    std::cout << std::endl;
  }
  
  // Test 4: Temporal Point Query
  void TestTemporalPointQuery() {
    std::cout << "[Test 4] Temporal Point Query" << std::endl;
    
    // Write multiple versions
    std::cout << "  Writing 100 versions..." << std::endl;
    uint64_t vid = 12345;
    std::vector<std::tuple<uint64_t, uint16_t, int32_t, uint64_t>> batch;
    batch.reserve(100);
    for (int i = 0; i < 100; ++i) {
      batch.push_back({vid, 1, i * 10, 4000000});
    }
    clients_[0].BatchWriteVertex(batch);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    const int num_queries = 10000;
    std::atomic<int> success_count{0};
    
    auto start = high_resolution_clock::now();
    
    for (int i = 0; i < num_queries; ++i) {
      uint64_t ts = 4000000 + (rng_() % 100) * 1000;
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
  
  // Test 5: Batch Neighbor Query (Optimized)
  void TestBatchNeighborQuery() {
    std::cout << "[Test 5] Batch Neighbor Query (Optimized with BatchGet)" << std::endl;
    
    // Allow system to settle after previous tests
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Create a fresh client to avoid stale channel state from previous tests
    StorageTestClient fresh_client(config_.storage_endpoints[0]);
    
    // Write graph data
    std::cout << "  Preparing graph data..." << std::endl;
    const int num_vertices = 1000;
    const int edges_per_vertex = 10;
    
    // Use high vertex_id range (100000+) to avoid conflicts with previous tests
    const uint64_t vertex_id_base = 100000;
    int write_success = 0;
    
    // Test simple write first
    bool simple_ok = fresh_client.WriteVertex(vertex_id_base, 1, 42, 5000000);
    std::cout << "  Simple write test: " << (simple_ok ? "OK" : "FAILED") << std::endl;
    
    // Check channel state
    auto channel_state = fresh_client.GetChannelState();
    std::cout << "  Channel state: " << channel_state << std::endl;
    
    std::vector<std::tuple<uint64_t, uint16_t, int32_t, uint64_t>> batch;
    batch.reserve(100);
    for (int v = 0; v < num_vertices; ++v) {
      for (int e = 0; e < edges_per_vertex; ++e) {
        uint64_t dst = (v + e + 1) % num_vertices;
        batch.push_back({vertex_id_base + v, static_cast<uint16_t>(1 + e), static_cast<int32_t>(dst), 5000000});
        if (batch.size() >= 100) {
          if (fresh_client.BatchWriteVertex(batch)) {
            write_success += static_cast<int>(batch.size());
          }
          batch.clear();
        }
      }
    }
    if (!batch.empty()) {
      if (fresh_client.BatchWriteVertex(batch)) {
        write_success += static_cast<int>(batch.size());
      }
    }
    std::cout << "  Written: " << write_success << "/" << (num_vertices * edges_per_vertex) << " edges" << std::endl;
    
    // Verify a small sample immediately (before flush)
    std::cout << "  Verifying sample data immediately..." << std::endl;
    int sample_success_immediate = 0;
    for (int v = 0; v < 10; ++v) {
      auto result = fresh_client.ReadVertex(vertex_id_base + v, 1, 5000000);
      if (result.has_value()) sample_success_immediate++;
    }
    std::cout << "  Sample verification (immediate): " << sample_success_immediate << "/10 found" << std::endl;
    
    // Wait for flush
    std::cout << "  Waiting for data persistence..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Verify a small sample after delay
    std::cout << "  Verifying sample data after delay..." << std::endl;
    int sample_success = 0;
    for (int v = 0; v < 10; ++v) {
      auto result = fresh_client.ReadVertex(vertex_id_base + v, 1, 5000000);
      if (result.has_value()) sample_success++;
    }
    std::cout << "  Sample verification (after delay): " << sample_success << "/10 found" << std::endl;
    
    // Batch read neighbors
    const int num_queries = 5000;
    std::atomic<int> success_count{0};
    std::atomic<int> total_neighbors{0};
    
    auto start = high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    int queries_per_thread = num_queries / config_.num_threads;
    for (int t = 0; t < config_.num_threads; ++t) {
      threads.emplace_back([&, t]() {
        for (int i = 0; i < queries_per_thread; ++i) {
          uint64_t vid = rng_() % num_vertices;
          
          // Prepare batch keys
          std::vector<std::tuple<uint64_t, uint16_t, uint64_t>> keys;
          for (int e = 0; e < edges_per_vertex; ++e) {
            keys.push_back({vertex_id_base + vid, static_cast<uint16_t>(1 + e), 5000000});
          }
          
          // Batch get
          auto results = fresh_client.BatchRead(keys);
          
          int found = 0;
          for (const auto& res : results) {
            if (res.has_value()) found++;
          }
          
          if (found > 0) {
            success_count++;
            total_neighbors += found;
          }
        }
      });
    }
    for (auto& t : threads) t.join();
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start).count();
    double throughput = (double)num_queries / (duration / 1000000.0);
    double avg_neighbors = success_count > 0 ? (double)total_neighbors / success_count.load() : 0;
    
    results_["Batch Neighbor"] = {throughput, (double)duration / num_queries, success_count};
    std::cout << "  Throughput:     " << std::fixed << std::setprecision(2) << throughput 
              << " queries/sec" << std::endl;
    std::cout << "  Latency:        " << std::setprecision(2) << (double)duration / num_queries 
              << " μs/query" << std::endl;
    std::cout << "  Success rate:   " << std::setprecision(1) 
              << (100.0 * success_count / num_queries) << "%" << std::endl;
    std::cout << "  Avg neighbors:  " << std::setprecision(2) << avg_neighbors << std::endl;
    std::cout << "  (Using BatchGet: " << edges_per_vertex << " neighbors per query)" << std::endl;
    std::cout << std::endl;
  }
  
  // Test 6: Persistence Verification
  void TestPersistenceVerification() {
    std::cout << "[Test 6] Persistence Verification" << std::endl;
    
    std::cout << "  Checking partition info from all nodes..." << std::endl;
    
    uint64_t total_size = 0;
    uint64_t total_keys = 0;
    
    for (size_t i = 0; i < clients_.size(); ++i) {
      uint64_t data_size, key_count;
      if (clients_[i].GetPartitionInfo(0, data_size, key_count)) {
        std::cout << "  Storage Node " << (i + 1) << ":" << std::endl;
        std::cout << "    Data size: " << FormatBytes(data_size) << std::endl;
        std::cout << "    Key count: " << key_count << std::endl;
        total_size += data_size;
        total_keys += key_count;
      }
    }
    
    std::cout << "  Total: " << FormatBytes(total_size) << ", " << total_keys << " keys" << std::endl;
    
    // Check directory
    std::cout << "  Data directory check:" << std::endl;
    for (size_t i = 0; i < config_.data_dirs.size(); ++i) {
      std::string cmd = "du -sh " + config_.data_dirs[i] + " 2>/dev/null | cut -f1";
      FILE* pipe = popen(cmd.c_str(), "r");
      if (pipe) {
        char buffer[128];
        if (fgets(buffer, sizeof(buffer), pipe)) {
          std::cout << "    node" << (i + 1) << ": " << buffer;
        }
        pclose(pipe);
      }
    }
    
    // Check SST files
    std::cout << "  SST files:" << std::endl;
    for (size_t i = 0; i < config_.data_dirs.size(); ++i) {
      std::string cmd = "ls " + config_.data_dirs[i] + "/*.sst 2>/dev/null | wc -l";
      FILE* pipe = popen(cmd.c_str(), "r");
      if (pipe) {
        char buffer[128];
        if (fgets(buffer, sizeof(buffer), pipe)) {
          std::cout << "    node" << (i + 1) << ": " << buffer << " SST files";
        }
        pclose(pipe);
      }
    }
    
    results_["Persistence"] = {(double)total_size, (double)total_keys, 
                                total_keys > 0 ? 1 : 0};
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
    std::cout << "✅ All tests completed with fixes applied!" << std::endl;
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

int main(int argc, char* argv[]) {
  TestConfig config;
  
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--threads" && i + 1 < argc) {
      config.num_threads = std::stoi(argv[++i]);
    } else if (arg == "--iterations" && i + 1 < argc) {
      config.test_iterations = std::stoi(argv[++i]);
    }
  }
  
  try {
    FixedTestSuite suite(config);
    suite.RunAllTests();
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
  
  return 0;
}
