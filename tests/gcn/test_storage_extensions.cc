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

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <filesystem>

#include "cedar/dtx/security.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "storage_service.pb.h"
#include "storage_service.grpc.pb.h"

namespace fs = std::filesystem;
using namespace cedar;

class StorageExtensionsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    data_dir_ = "/tmp/cedar_storage_extensions_test_" + std::to_string(getpid());
    fs::remove_all(data_dir_);
    fs::create_directories(data_dir_);

    // Disable auth for unit tests so gRPC calls do not require Bearer tokens.
    {
      cedar::dtx::security::SecurityManager::Config sec_cfg;
      sec_cfg.enable_auth = false;
      cedar::dtx::security::SecurityManager::GetInstance()->Initialize(sec_cfg);
    }

    partition_manager_ = std::make_unique<dtx::StoragePartitionManager>();
    dtx::StoragePartitionManager::PartitionConfig pm_config;
    pm_config.data_root = data_dir_;
    pm_config.max_partitions = 1024;

    Status status = partition_manager_->Initialize(pm_config);
    ASSERT_TRUE(status.ok()) << status.ToString();

    status = partition_manager_->AddPartition(0);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Insert test edges directly into the shared storage
    InsertTestEdges();

    // Start in-process gRPC server
    service_ = std::make_unique<dtx::StorageServiceImpl>(partition_manager_.get());
    server_address_ = "127.0.0.1:0";

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials(), &port_);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);

    std::ostringstream oss;
    oss << "127.0.0.1:" << port_;
    server_address_ = oss.str();

    server_thread_ = std::thread([this]() { server_->Wait(); });

    auto channel = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
    stub_ = cedar::storage::StorageService::NewStub(channel);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
    }
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
    if (partition_manager_) {
      partition_manager_->Shutdown();
    }
    fs::remove_all(data_dir_);
  }

  void InsertTestEdges() {
    // Entity 100 maps to partition 100 (low 16 bits), not partition 0
    auto* partition = partition_manager_->GetPartition(100);
    if (!partition) {
      // Add partition 100 if not already present
      ASSERT_TRUE(partition_manager_->AddPartition(100).ok());
      partition = partition_manager_->GetPartition(100);
    }
    ASSERT_NE(partition, nullptr);
    auto* storage = partition->GetEffectiveStorage();
    ASSERT_NE(storage, nullptr);

    // Insert outgoing edges from entity 100 to various targets
    for (uint64_t dst = 200; dst < 205; ++dst) {
      Status s = storage->PutEdge(
          100, dst, 1, Timestamp(1000), Descriptor::InlineInt(1, 42), Timestamp(1000));
      ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // Insert incoming edges to entity 100 from various sources
    for (uint64_t src = 300; src < 303; ++src) {
      Status s = storage->PutEdge(
          src, 100, 2, Timestamp(1000), Descriptor::InlineInt(1, 42), Timestamp(1000));
      ASSERT_TRUE(s.ok()) << s.ToString();
    }
  }

  std::string data_dir_;
  std::unique_ptr<dtx::StoragePartitionManager> partition_manager_;
  std::unique_ptr<dtx::StorageServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  std::thread server_thread_;
  std::string server_address_;
  int port_ = 0;
  std::unique_ptr<cedar::storage::StorageService::Stub> stub_;
};

TEST_F(StorageExtensionsTest, GetCommittedVersionReturnsOk) {
  cedar::storage::GetCommittedVersionRequest request;
  cedar::storage::GetCommittedVersionResponse response;
  grpc::ClientContext context;

  grpc::Status status = stub_->GetCommittedVersion(&context, request, &response);

  EXPECT_TRUE(status.ok());
  // committed_version returns the current wall-clock timestamp; we only verify
  // the RPC succeeds and returns a non-zero version.
  EXPECT_GT(response.committed_version(), 0u);
  EXPECT_GT(response.watermark(), 0u);
}

TEST_F(StorageExtensionsTest, GetRangeForComputeOutEdges) {
  cedar::storage::GetRangeForComputeRequest request;
  request.set_entity_id(100);
  request.set_edge_type(1);
  request.set_snapshot_ts(2000);
  request.set_direction(0);  // out
  request.set_required_version(0);

  cedar::storage::GetRangeForComputeResponse response;
  grpc::ClientContext context;

  grpc::Status status = stub_->GetRangeForCompute(&context, request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(response.edges_size(), 5);
  EXPECT_FALSE(response.truncated());

  std::set<uint64_t> target_ids;
  for (const auto& edge : response.edges()) {
    target_ids.insert(edge.target_id());
    EXPECT_EQ(edge.edge_type(), 1u);
    EXPECT_EQ(edge.valid_from(), 1000u);
    EXPECT_EQ(edge.valid_to(), 0u);
  }
  EXPECT_EQ(target_ids, (std::set<uint64_t>{200, 201, 202, 203, 204}));
}

TEST_F(StorageExtensionsTest, GetRangeForComputeInEdges) {
  cedar::storage::GetRangeForComputeRequest request;
  request.set_entity_id(100);
  request.set_edge_type(2);
  request.set_snapshot_ts(2000);
  request.set_direction(1);  // in
  request.set_required_version(0);

  cedar::storage::GetRangeForComputeResponse response;
  grpc::ClientContext context;

  grpc::Status status = stub_->GetRangeForCompute(&context, request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(response.edges_size(), 3);
  EXPECT_FALSE(response.truncated());

  std::set<uint64_t> target_ids;
  for (const auto& edge : response.edges()) {
    target_ids.insert(edge.target_id());
    EXPECT_EQ(edge.edge_type(), 2u);
  }
  EXPECT_EQ(target_ids, (std::set<uint64_t>{300, 301, 302}));
}

TEST_F(StorageExtensionsTest, GetRangeForComputeEmptyForNonExistentEntity) {
  cedar::storage::GetRangeForComputeRequest request;
  request.set_entity_id(99999);
  request.set_edge_type(1);
  request.set_snapshot_ts(2000);
  request.set_direction(0);
  request.set_required_version(0);

  cedar::storage::GetRangeForComputeResponse response;
  grpc::ClientContext context;

  grpc::Status status = stub_->GetRangeForCompute(&context, request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(response.edges_size(), 0);
  EXPECT_FALSE(response.truncated());
}
