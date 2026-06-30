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
#include <chrono>
#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/meta_service_grpc.h"
#include "cedar/dtx/unified_meta_client.h"
#include "../test_tls_certs.h"

using namespace cedar::dtx;

class MetaServiceGrpcClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(cedar::test::SetupTestTlsEnvironment("meta_service_grpc_client"));

    MetaServiceConfig config;
    config.node_id = 1;
    config.listen_address = "localhost:19559";
    config.advertise_address = "localhost:19559";
    config.test_mode = true;

    auto status = meta_service_.Initialize(config);
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto grpc_status = grpc_server_.Start("localhost:19559", &meta_service_);
    ASSERT_TRUE(grpc_status.ok()) << grpc_status.ToString();

    auto connect_status = client_.Connect({"localhost:19559"});
    ASSERT_TRUE(connect_status.ok()) << connect_status.ToString();
  }

  void TearDown() override {
    grpc_server_.Stop();
    meta_service_.Shutdown();
  }

  MetadataService meta_service_;
  MetaServiceGrpcServer grpc_server_;
  MetaServiceGrpcClient client_;
};

TEST_F(MetaServiceGrpcClientTest, GetSpacePartitionMapReturnsAssignments) {
  // Register three nodes so that CreateSpace can assign replicas
  for (NodeID nid = 100; nid <= 102; ++nid) {
    NodeInfo node;
    node.node_id = nid;
    node.address = "10.0.0." + std::to_string(nid - 99) + ":50051";
    node.data_path = "/data/cedar";
    auto reg_status = client_.RegisterNode(node);
    ASSERT_TRUE(reg_status.ok()) << reg_status.ToString();
  }

  // Create a space with 4 partitions and replication factor 3
  SpaceDef space;
  space.name = "test_space";
  space.partition_num = 4;
  space.replica_factor = 3;

  cedar::meta::CreateSpaceRequest create_req;
  auto* space_proto = create_req.mutable_space();
  space_proto->set_name(space.name);
  space_proto->set_partition_num(space.partition_num);
  space_proto->set_replica_factor(space.replica_factor);

  auto stub = client_.GetStub();
  ASSERT_NE(stub, nullptr);

  grpc::ClientContext ctx;
  cedar::meta::CreateSpaceResponse create_resp;
  auto grpc_status = stub->CreateSpace(&ctx, create_req, &create_resp);
  ASSERT_TRUE(grpc_status.ok());
  ASSERT_TRUE(create_resp.success());

  // Now call GetSpacePartitionMap via the client
  auto result = client_.GetSpacePartitionMap("test_space");
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  auto& map = result.value();
  ASSERT_FALSE(map.assignments.empty()) << "assignments should not be empty";
  EXPECT_EQ(map.assignments.size(), 4);
  for (const auto& [partition_id, assign] : map.assignments) {
    EXPECT_EQ(assign.partition_id, partition_id);
    EXPECT_EQ(assign.space_name, "test_space");
    EXPECT_NE(assign.leader_node, kInvalidNodeID)
        << "partition " << partition_id << " has no leader";
    EXPECT_EQ(assign.follower_nodes.size(), 2)
        << "partition " << partition_id << " should have 2 followers";
  }
}

TEST_F(MetaServiceGrpcClientTest, DestructorWakesHealthMonitorThreadPromptly) {
  auto client = std::make_unique<MetaServiceGrpcClient>();
  auto connect_status = client->Connect({"localhost:19559"});
  ASSERT_TRUE(connect_status.ok()) << connect_status.ToString();

  auto start = std::chrono::steady_clock::now();
  client.reset();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 500);
}

TEST_F(MetaServiceGrpcClientTest, UnifiedMetaClientShutdownWakesRefreshThreadPromptly) {
  for (NodeID nid = 200; nid <= 202; ++nid) {
    NodeInfo node;
    node.node_id = nid;
    node.address = "10.1.0." + std::to_string(nid - 199) + ":50051";
    node.data_path = "/data/cedar";
    auto reg_status = client_.RegisterNode(node);
    ASSERT_TRUE(reg_status.ok()) << reg_status.ToString();
  }

  cedar::meta::CreateSpaceRequest create_req;
  auto* space_proto = create_req.mutable_space();
  space_proto->set_name("unified_meta_shutdown_space");
  space_proto->set_partition_num(2);
  space_proto->set_replica_factor(3);

  auto stub = client_.GetStub();
  ASSERT_NE(stub, nullptr);

  grpc::ClientContext ctx;
  cedar::meta::CreateSpaceResponse create_resp;
  auto grpc_status = stub->CreateSpace(&ctx, create_req, &create_resp);
  ASSERT_TRUE(grpc_status.ok());
  ASSERT_TRUE(create_resp.success()) << create_resp.error_msg();

  UnifiedMetaClient::Options options;
  options.meta_service_address = "localhost:19559";
  options.space_name = "unified_meta_shutdown_space";
  options.refresh_interval = std::chrono::seconds(30);
  options.rpc_timeout = std::chrono::milliseconds(2000);
  options.enable_watch = false;

  UnifiedMetaClient client(options);
  auto init_status = client.Init();
  ASSERT_TRUE(init_status.ok()) << init_status.ToString();

  auto start = std::chrono::steady_clock::now();
  auto shutdown_status = client.Shutdown();
  auto elapsed = std::chrono::steady_clock::now() - start;

  ASSERT_TRUE(shutdown_status.ok()) << shutdown_status.ToString();
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 500);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
