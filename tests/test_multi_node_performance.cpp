// Copyright 2025 The Cedar Authors
// Multi-Node Performance Test for CedarGraph (3/5/7 nodes)

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
#include <future>

#include <grpcpp/grpcpp.h>
#include "storage_service.pb.h"
#include "storage_service.grpc.pb.h"
#include "cedar/types/descriptor.h"
#include "cedar/core/slice.h"

using namespace std::chrono;

// =============================================================================
// Node Configuration Templates
// =============================================================================
struct NodeConfig {
  int node_count;
  std::vector<std::string> storage_endpoints;
  std::vector<std::string> metad_endpoints;
};

NodeConfig Get3NodeConfig() {
  return {
    3,
    {"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"},
    {"127.0.0.1:2379", "127.0.0.1:2380", "127.0.0.1:2381"}
  };
}

NodeConfig Get5NodeConfig() {
  return {
    5,
    {"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003", 
     "127.0.0.1:7004", "127.0.0.1:7005"},
    {"127.0.0.1:2379", "127.0.0.1:2380", "127.0.0.1:2381",
     "127.0.0.1:2382", "127.0.0.1:2383"}
  };
}

NodeConfig Get7NodeConfig() {
  return {
    7,
    {"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003",
     "127.0.0.1:7004", "127.0.0.1:7005", "127.0.0.1:7006",
     "127.0.0.1:7007"},
    {"127.0.0.1:2379", "127.0.0.1:2380", "127.0.0.1:2381",
     "127.0.0.1:2382", "127.0.0.1:2383", "127.0.0.1:2384",
     "127.0.0.1:2385"}
  };
}

// =============================================================================
// Storage Client
// =============================================================================
class StorageTestClient {
 public:
  StorageTestClient(const std::string& endpoint) 
      : endpoint_(endpoint), connected_(false) {
    channel_ = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    stub_ = cedar::storage::StorageService::NewStub(channel_);
    
    // Test connection
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
    connected_ = channel_->WaitForConnected(deadline);
  }
  
  bool IsConnected() const { return connected_; }
  
  const std::string& GetEndpoint() const { return endpoint_; }
  
  // Write a single value
  bool Write(uint64_t key_id, int32_t value, uint64_t timestamp) {
    cedar::storage::PutRequest request;
    auto* key = request.mutable_key();
    key->set_entity_id(key_id);
    key->set_timestamp(timestamp);
    key->set_column_id(1);
    key->set_type_flags(0);
    key->set_partition_id(key_id % 4);  // Distribute across partitions
    
    auto* desc = request.mutable_descriptor_();
    auto descriptor = cedar::Descriptor::InlineInt(1, value);
    auto encoded = descriptor.Encode();
    desc->set_data(encoded);
    
    request.mutable_txn_version()->set_value(timestamp);
    request.set_txn_id(0);
    
    cedar::storage::PutResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    
    auto status = stub_->Put(&context, request, &response);
    return status.ok() && response.success();
  }
  
  // Read a value
  std::optional<int32_t> Read(uint64_t key_id, uint64_t timestamp) {
    cedar::storage::GetRequest request;
    auto* key = request.mutable_key();
    key->set_entity_id(key_id);
    key->set_timestamp(timestamp);
    key->set_column_id(1);
    key->set_type_flags(0);
    key->set_partition_id(key_id % 4);
    
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
  
  // Batch write
  bool BatchWrite(const std::vector<std::pair<uint64_t, int32_t>>& items, uint64_t timestamp) {
    cedar::storage::BatchPutRequest request;
    
    for (const auto& [key_id, value] : items) {
      auto* item = request.add_items();
      auto* key = item->mutable_key();
      key->set_entity_id(key_id);
      key->set_timestamp(timestamp);
      key->set_column_id(1);
      key->set_type_flags(0);
      key->set_partition_id(key_id % 4);
      
      auto* desc = item->mutable_descriptor_();
      auto descriptor = cedar::Descriptor::InlineInt(1, value);
      auto encoded = descriptor.Encode();
      desc->set_data(encoded);
    }
    
    request.mutable_txn_version()->set_value(timestamp);
    request.set_txn_id(0);
    
    cedar::storage::BatchPutResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    
    auto status = stub_->BatchPut(&context, request, &response);
    return status.ok() && response.success();
  }
  
  // 2PC Prepare
  bool Prepare(uint64_t txn_id, const std::vector<uint64_t>& write_set, uint64_t commit_ts) {
    cedar::storage::PrepareRequest request;
    request.set_txn_id(txn_id);
    request.set_commit_ts(commit_ts);
    
    for (auto id : write_set) {
      auto* key = request.add_write_set();
      key->set_entity_id(id);
      key->set_partition_id(id % 4);
    }
    
    cedar::storage::PrepareResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    
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
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    
    auto status = stub_->Commit(&context, request, &response);
    return status.ok() && response.success();
  }
  
 private:
  std::string endpoint_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<cedar::storage::StorageService::Stub> stub_;
  bool connected_;
};

// =============================================================================
// Multi-Node Test Suite
// =============================================================================
class MultiNodeTestSuite {
 public:
  struct TestConfig {
    int node_count;
    int num_operations = 10000;
    int num_threads = 4;
    int batch_size = 100;
    int warmup_ops = 1000;
  };
  
  struct TestResult {
    double throughput;  // ops/sec
    double latency;     // μs/op
    int success_count;
    int total_count;
    double p50_latency;
    double p99_latency;
  };
  
  MultiNodeTestSuite(const NodeConfig& config, const TestConfig& test_config)
      : node_config_(config), test_config_(test_config) {
    
    std::cout << "Connecting to " << config.node_count << " storage nodes..." << std::endl;
    
    int connected = 0;
    for (const auto& endpoint : config.storage_endpoints) {
      auto client = std::make_unique<StorageTestClient>(endpoint);
      if (client->IsConnected()) {
        clients_.push_back(std::move(client));
        connected++;
      } else {
        std::cout << "  Warning: Failed to connect to " << endpoint << std::endl;
      }
    }
    
    std::cout << "  Connected: " << connected << "/" << config.node_count << std::endl;
    
    if (clients_.empty()) {
      throw std::runtime_error("No storage nodes available");
    }
    
    rng_.seed(42);
  }
  
  void RunAllTests(bool only_2pc = false) {
    std::cout << std::endl;
    std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║     CedarGraph Multi-Node Performance Test (" 
              << std::setw(2) << node_config_.node_count << " Nodes)         ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    
    if (only_2pc) {
      auto txn_result = Test2PCTransactions();
      std::cout << std::endl;
      std::cout << "2PC (txns/sec): " << txn_result.throughput 
                << "  Avg: " << txn_result.latency << " µs"
                << "  P50: " << txn_result.p50_latency << " µs"
                << "  P99: " << txn_result.p99_latency << " µs"
                << std::endl;
      return;
    }
    
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
  }
  
 private:
  void RunWarmup() {
    for (int i = 0; i < test_config_.warmup_ops; ++i) {
      uint64_t key = rng_() % 1000;
      int32_t value = rng_();
      clients_[i % clients_.size()]->Write(key, value, 1000000 + i);
    }
  }
  
  TestResult TestWriteThroughput() {
    std::cout << "[Test 1] Single Write Throughput" << std::endl;
    
    std::atomic<int> success_count{0};
    std::vector<double> latencies;
    std::mutex latency_mutex;
    
    auto start = high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    int ops_per_thread = test_config_.num_operations / test_config_.num_threads;
    
    for (int t = 0; t < test_config_.num_threads; ++t) {
      threads.emplace_back([&, t]() {
        for (int i = 0; i < ops_per_thread; ++i) {
          uint64_t key = (t * ops_per_thread + i) % 10000;
          int32_t value = rng_();
          uint64_t ts = 2000000 + i;
          
          auto op_start = high_resolution_clock::now();
          bool success = clients_[i % clients_.size()]->Write(key, value, ts);
          auto op_end = high_resolution_clock::now();
          
          if (success) {
            success_count++;
            double latency = duration_cast<nanoseconds>(op_end - op_start).count() / 1000.0;
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies.push_back(latency);
          }
        }
      });
    }
    
    for (auto& t : threads) t.join();
    
    auto end = high_resolution_clock::now();
    auto duration_us = duration_cast<microseconds>(end - start).count();
    
    return CalculateResult(success_count, test_config_.num_operations, duration_us, latencies);
  }
  
  TestResult TestReadThroughput() {
    std::cout << "[Test 2] Single Read Throughput" << std::endl;
    
    // Pre-write data using key-based client routing so reads hit the same node
    std::cout << "  Preparing data..." << std::endl;
    for (int i = 0; i < 5000; ++i) {
      size_t client_idx = i % clients_.size();
      clients_[client_idx]->Write(i, i * 100, 3000000);
    }
    
    std::atomic<int> success_count{0};
    std::vector<double> latencies;
    std::mutex latency_mutex;
    
    auto start = high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    int ops_per_thread = test_config_.num_operations / test_config_.num_threads;
    
    for (int t = 0; t < test_config_.num_threads; ++t) {
      threads.emplace_back([&, t]() {
        for (int i = 0; i < ops_per_thread; ++i) {
          uint64_t key = rng_() % 5000;
          size_t client_idx = key % clients_.size();
          
          auto op_start = high_resolution_clock::now();
          auto result = clients_[client_idx]->Read(key, 3000000);
          auto op_end = high_resolution_clock::now();
          
          if (result.has_value()) {
            success_count++;
            double latency = duration_cast<nanoseconds>(op_end - op_start).count() / 1000.0;
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies.push_back(latency);
          }
        }
      });
    }
    
    for (auto& t : threads) t.join();
    
    auto end = high_resolution_clock::now();
    auto duration_us = duration_cast<microseconds>(end - start).count();
    
    return CalculateResult(success_count, test_config_.num_operations, duration_us, latencies);
  }
  
  TestResult TestBatchWrite() {
    std::cout << "[Test 3] Batch Write Throughput (batch_size=" 
              << test_config_.batch_size << ")" << std::endl;
    
    std::atomic<int> success_count{0};
    std::atomic<int> total_batches{0};
    std::vector<double> latencies;
    std::mutex latency_mutex;
    
    int num_batches = test_config_.num_operations / test_config_.batch_size;
    
    auto start = high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    int batches_per_thread = num_batches / test_config_.num_threads;
    
    for (int t = 0; t < test_config_.num_threads; ++t) {
      threads.emplace_back([&, t]() {
        for (int i = 0; i < batches_per_thread; ++i) {
          std::vector<std::pair<uint64_t, int32_t>> batch;
          for (int j = 0; j < test_config_.batch_size; ++j) {
            batch.push_back({(t * batches_per_thread + i) * test_config_.batch_size + j, rng_()});
          }
          
          auto op_start = high_resolution_clock::now();
          bool success = clients_[i % clients_.size()]->BatchWrite(batch, 4000000 + i);
          auto op_end = high_resolution_clock::now();
          
          total_batches++;
          if (success) {
            success_count++;
            double latency = duration_cast<nanoseconds>(op_end - op_start).count() / 1000.0;
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies.push_back(latency);
          }
        }
      });
    }
    
    for (auto& t : threads) t.join();
    
    auto end = high_resolution_clock::now();
    auto duration_us = duration_cast<microseconds>(end - start).count();
    
    // For batch, count individual operations
    int total_ops = total_batches * test_config_.batch_size;
    int success_ops = success_count * test_config_.batch_size;
    
    TestResult result = CalculateResult(success_ops, total_ops, duration_us, latencies);
    result.throughput = (double)success_ops / (duration_us / 1000000.0);
    return result;
  }
  
  TestResult Test2PCTransactions() {
    std::cout << "[Test 4] 2PC Transaction Throughput" << std::endl;
    
    int num_txns = test_config_.num_operations / 10;  // Fewer transactions
    std::atomic<int> success_count{0};
    std::atomic<int> prepared_count{0};
    std::vector<double> latencies;
    std::mutex latency_mutex;
    
    auto start = high_resolution_clock::now();
    
    for (int t = 0; t < num_txns; ++t) {
      uint64_t txn_id = 10000 + t;
      std::vector<uint64_t> write_set;
      
      // Generate write set
      for (int i = 0; i < 5; ++i) {
        write_set.push_back(rng_() % 1000);
      }
      
      uint64_t commit_ts = 5000000 + t;
      
      auto op_start = high_resolution_clock::now();
      
      // Phase 1: Prepare (concurrent to all nodes)
      std::vector<std::future<bool>> prepare_futures;
      for (auto& client : clients_) {
        prepare_futures.push_back(
            std::async(std::launch::async, [&client, txn_id, &write_set, commit_ts]() {
              return client->Prepare(txn_id, write_set, commit_ts);
            }));
      }
      int prepare_ok = 0;
      for (auto& f : prepare_futures) {
        if (f.get()) prepare_ok++;
      }
      
      if (prepare_ok == clients_.size()) {
        prepared_count++;
        
        // Phase 2: Commit (concurrent to all nodes)
        std::vector<std::future<bool>> commit_futures;
        for (auto& client : clients_) {
          commit_futures.push_back(
              std::async(std::launch::async, [&client, txn_id, commit_ts]() {
                return client->Commit(txn_id, commit_ts);
              }));
        }
        int commit_ok = 0;
        for (auto& f : commit_futures) {
          if (f.get()) commit_ok++;
        }
        
        if (commit_ok == clients_.size()) {
          success_count++;
          auto op_end = high_resolution_clock::now();
          double latency = duration_cast<nanoseconds>(op_end - op_start).count() / 1000.0;
          std::lock_guard<std::mutex> lock(latency_mutex);
          latencies.push_back(latency);
        }
      }
    }
    
    auto end = high_resolution_clock::now();
    auto duration_us = duration_cast<microseconds>(end - start).count();
    
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
    
    // Calculate percentiles
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
    std::cout << "║           Performance Summary (" << std::setw(2) << node_config_.node_count 
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
  
  NodeConfig node_config_;
  TestConfig test_config_;
  std::vector<std::unique_ptr<StorageTestClient>> clients_;
  std::mt19937 rng_;
};

// =============================================================================
// Main Entry Point
// =============================================================================
int main(int argc, char* argv[]) {
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     CedarGraph Multi-Node Performance Test Suite           ║" << std::endl;
  std::cout << "║              (3-node / 5-node / 7-node)                    ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  // Default test configuration
  MultiNodeTestSuite::TestConfig test_config;
  test_config.num_operations = 5000;  // Reduced for faster testing
  test_config.num_threads = 4;
  test_config.batch_size = 50;
  test_config.warmup_ops = 500;
  
  // Parse arguments
  std::vector<int> node_configs = {3, 5, 7};  // Default: test all
  bool only_2pc = false;
  
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--nodes" && i + 1 < argc) {
      int nodes = std::stoi(argv[++i]);
      node_configs = {nodes};
    } else if (arg == "--ops" && i + 1 < argc) {
      test_config.num_operations = std::stoi(argv[++i]);
    } else if (arg == "--threads" && i + 1 < argc) {
      test_config.num_threads = std::stoi(argv[++i]);
    } else if (arg == "--only-2pc") {
      only_2pc = true;
    } else if (arg == "--help") {
      std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
      std::cout << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  --nodes N       Test with N nodes (3/5/7, default: all)" << std::endl;
      std::cout << "  --ops N         Number of operations (default: 5000)" << std::endl;
      std::cout << "  --threads N     Number of threads (default: 4)" << std::endl;
      std::cout << "  --only-2pc      Only run 2PC transaction test" << std::endl;
      std::cout << std::endl;
      return 0;
    }
  }
  
  // Run tests for each node configuration
  for (int node_count : node_configs) {
    try {
      NodeConfig config;
      switch (node_count) {
        case 3: config = Get3NodeConfig(); break;
        case 5: config = Get5NodeConfig(); break;
        case 7: config = Get7NodeConfig(); break;
        default:
          std::cerr << "Unsupported node count: " << node_count << std::endl;
          continue;
      }
      
      config.node_count = node_count;
      MultiNodeTestSuite suite(config, test_config);
      suite.RunAllTests(only_2pc);
      
      if (node_count != node_configs.back()) {
        std::cout << std::endl << std::string(60, '=') << std::endl << std::endl;
      }
      
    } catch (const std::exception& e) {
      std::cerr << "Error testing " << node_count << " nodes: " << e.what() << std::endl;
    }
  }
  
  std::cout << std::endl;
  std::cout << "All tests completed!" << std::endl;
  return 0;
}
