// Simple verification test for CedarGraph Distributed
// Self-contained: starts an embedded gRPC storage server for testing.
#include <iostream>
#include <chrono>
#include <thread>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/storage/cedar_options.h"
#include "storage_service.pb.h"
#include "storage_service.grpc.pb.h"

namespace fs = std::filesystem;
using namespace std::chrono;

int main() {
  std::cout << "╔════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     CedarGraph Distributed Simple Verification         ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;

  // Clean up and create temporary data directory
  std::string data_dir = "/tmp/cedar_distributed_test_node1";
  fs::remove_all(data_dir);
  fs::create_directories(data_dir);

  // Initialize StoragePartitionManager
  auto partition_manager = std::make_unique<cedar::dtx::StoragePartitionManager>();
  cedar::dtx::StoragePartitionManager::PartitionConfig pm_config;
  pm_config.data_root = data_dir;
  pm_config.max_partitions = 1024;

  cedar::Status status = partition_manager->Initialize(pm_config);
  if (!status.ok()) {
    std::cerr << "Failed to initialize partition manager: " << status.ToString() << std::endl;
    return 1;
  }

  status = partition_manager->AddPartition(0);
  if (!status.ok()) {
    std::cerr << "Failed to create partition 0: " << status.ToString() << std::endl;
    return 1;
  }

  // Start embedded gRPC server on a fixed local port
  cedar::dtx::StorageServiceImpl grpc_service(partition_manager.get());
  std::string server_address = "127.0.0.1:27001";
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&grpc_service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();

  if (!server) {
    std::cerr << "Failed to start embedded gRPC server on " << server_address << std::endl;
    return 1;
  }

  std::thread server_thread([&server]() { server->Wait(); });
  std::cout << "[Setup] Embedded storage server running on " << server_address << std::endl;
  std::cout << std::endl;

  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  auto stub = cedar::storage::StorageService::NewStub(channel);

  // Test 1: Basic Write/Read
  std::cout << "[Test 1] Basic Write/Read Verification" << std::endl;

  uint64_t entity_id = 99999;
  uint64_t timestamp = 2000000;
  uint16_t column_id = 1;
  int32_t write_value = 424242;

  // Write
  {
    cedar::storage::PutRequest req;
    req.mutable_key()->set_entity_id(entity_id);
    req.mutable_key()->set_timestamp(timestamp);
    req.mutable_key()->set_column_id(column_id);
    req.mutable_key()->set_type_flags(0 << 16);
    req.mutable_key()->set_partition_id(0);

    req.mutable_value_descriptor()->set_data(
        reinterpret_cast<const char*>(&write_value), sizeof(write_value));
    req.mutable_txn_version()->set_value(timestamp);
    req.set_txn_id(0);

    cedar::storage::PutResponse resp;
    grpc::ClientContext ctx;
    auto grpc_status = stub->Put(&ctx, req, &resp);

    if (!grpc_status.ok()) {
      std::cout << "  Write: ❌ FAILED" << std::endl;
      std::cout << "    gRPC Error: " << grpc_status.error_message() << std::endl;
    } else if (!resp.success()) {
      std::cout << "  Write: ❌ FAILED" << std::endl;
      std::cout << "    Error: " << resp.error_msg() << std::endl;
    } else {
      std::cout << "  Write: ✅ SUCCESS" << std::endl;
    }
  }

  // Read back
  {
    cedar::storage::GetRequest req;
    req.mutable_key()->set_entity_id(entity_id);
    req.mutable_key()->set_timestamp(timestamp);
    req.mutable_key()->set_column_id(column_id);
    req.mutable_key()->set_type_flags(0 << 16);
    req.mutable_key()->set_partition_id(0);

    cedar::storage::GetResponse resp;
    grpc::ClientContext ctx;
    auto grpc_status = stub->Get(&ctx, req, &resp);

    if (!grpc_status.ok()) {
      std::cout << "  Read:  ❌ FAILED" << std::endl;
      std::cout << "    gRPC Error: " << grpc_status.error_message() << std::endl;
    } else if (!resp.success() || !resp.found()) {
      std::cout << "  Read:  ❌ FAILED" << std::endl;
      std::cout << "  Found: " << resp.found() << std::endl;
      std::cout << "  ❌ Data not found" << std::endl;
    } else {
      std::cout << "  Read:  ✅ SUCCESS" << std::endl;
      std::cout << "  Found: " << resp.found() << std::endl;
      if (resp.has_value_descriptor()) {
        const auto& data = resp.value_descriptor().data();
        if (data.size() >= sizeof(int32_t)) {
          int32_t read_value = *reinterpret_cast<const int32_t*>(data.data());
          std::cout << "  Value: " << read_value << " (expected: " << write_value << ")" << std::endl;
          if (read_value == write_value) {
            std::cout << "  ✅ Value matches!" << std::endl;
          } else {
            std::cout << "  ❌ Value mismatch!" << std::endl;
          }
        }
      }
    }
  }

  // Test 2: Throughput Test
  std::cout << std::endl;
  std::cout << "[Test 2] Write Throughput Test" << std::endl;

  const int num_writes = 10000;
  auto start = high_resolution_clock::now();

  int success = 0;
  for (int i = 0; i < num_writes; ++i) {
    cedar::storage::PutRequest req;
    req.mutable_key()->set_entity_id(100000 + i);
    req.mutable_key()->set_timestamp(3000000 + i);
    req.mutable_key()->set_column_id(1);
    req.mutable_key()->set_type_flags(0 << 16);
    req.mutable_key()->set_partition_id(0);

    int32_t val = i;
    req.mutable_value_descriptor()->set_data(
        reinterpret_cast<const char*>(&val), sizeof(val));
    req.mutable_txn_version()->set_value(3000000 + i);

    cedar::storage::PutResponse resp;
    grpc::ClientContext ctx;
    auto grpc_status = stub->Put(&ctx, req, &resp);

    if (grpc_status.ok() && resp.success()) {
      success++;
    }
  }

  auto end = high_resolution_clock::now();
  auto duration = duration_cast<microseconds>(end - start).count();

  double throughput = (double)success / (duration / 1000000.0);
  double latency = (double)duration / num_writes;

  std::cout << "  Writes:     " << success << "/" << num_writes << std::endl;
  std::cout << "  Throughput: " << throughput << " ops/sec" << std::endl;
  std::cout << "  Latency:    " << latency << " μs/op" << std::endl;

  // Test 3: 2PC Transaction Test
  std::cout << std::endl;
  std::cout << "[Test 3] 2PC Transaction Test" << std::endl;

  uint64_t txn_id = 999999;
  uint64_t commit_ts = 4000000;

  // Prepare
  {
    cedar::storage::PrepareRequest req;
    req.set_txn_id(txn_id);
    req.set_commit_ts(commit_ts);

    auto* read_key = req.add_read_set();
    read_key->set_entity_id(99999);
    read_key->set_partition_id(0);

    auto* write_key = req.add_write_set();
    write_key->set_entity_id(88888);
    write_key->set_partition_id(0);

    cedar::storage::PrepareResponse resp;
    grpc::ClientContext ctx;
    auto grpc_status = stub->Prepare(&ctx, req, &resp);

    if (!grpc_status.ok()) {
      std::cout << "  Prepare: ❌ FAILED (gRPC: " << grpc_status.error_message() << ")" << std::endl;
    } else if (!resp.prepared()) {
      std::cout << "  Prepare: ❌ FAILED (" << resp.error_msg() << ")" << std::endl;
    } else {
      std::cout << "  Prepare: ✅ SUCCESS" << std::endl;

      cedar::storage::CommitRequest commit_req;
      commit_req.set_txn_id(txn_id);
      commit_req.set_commit_ts(commit_ts);

      cedar::storage::CommitResponse commit_resp;
      grpc::ClientContext commit_ctx;
      auto commit_status = stub->Commit(&commit_ctx, commit_req, &commit_resp);

      if (commit_status.ok() && commit_resp.success()) {
        std::cout << "  Commit:  ✅ SUCCESS" << std::endl;
      } else {
        std::cout << "  Commit:  ❌ FAILED" << std::endl;
        if (!commit_status.ok()) {
          std::cout << "    gRPC Error: " << commit_status.error_message() << std::endl;
        } else {
          std::cout << "    Error: " << commit_resp.error_msg() << std::endl;
        }
      }
    }
  }

  // Test 4: Space Usage
  std::cout << std::endl;
  std::cout << "[Test 4] Space Usage" << std::endl;

  {
    cedar::storage::GetPartitionInfoRequest req;
    req.set_partition_id(0);

    cedar::storage::GetPartitionInfoResponse resp;
    grpc::ClientContext ctx;
    auto grpc_status = stub->GetPartitionInfo(&ctx, req, &resp);

    if (grpc_status.ok() && resp.success()) {
      std::cout << "  Data size: " << resp.info().data_size() << " bytes" << std::endl;
      std::cout << "  Key count: " << resp.info().key_count() << std::endl;
      std::cout << "  Is leader: " << (resp.info().is_leader() ? "Yes" : "No") << std::endl;
    } else {
      std::cout << "  GetPartitionInfo: ❌ FAILED" << std::endl;
    }
  }

  // Check directory
  std::cout << std::endl;
  std::cout << "[Test 5] Data Directory Check" << std::endl;
  std::string cmd = "du -sh " + data_dir + " 2>/dev/null | head -1";
  system(cmd.c_str());
  cmd = "ls -la " + data_dir + "/ 2>/dev/null | head -5";
  system(cmd.c_str());

  // Shutdown
  std::cout << std::endl;
  std::cout << "[Cleanup] Shutting down embedded server..." << std::endl;
  server->Shutdown();
  server_thread.join();
  partition_manager->Shutdown();
  fs::remove_all(data_dir);

  std::cout << std::endl;
  std::cout << "✅ Verification complete!" << std::endl;

  return 0;
}
