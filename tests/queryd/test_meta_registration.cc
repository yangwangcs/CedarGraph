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
// QueryD MetaClient Registration and Heartbeat Test
// =============================================================================
// Verifies that QueryMetaClient::RegisterQueryD and Heartbeat perform
// actual gRPC calls to the MetaService.
// =============================================================================

#include <gtest/gtest.h>

#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "cedar/queryd/meta_client.h"
#include "meta_service.grpc.pb.h"
#include "meta_service.pb.h"

using namespace cedar;
using namespace cedar::queryd;

class MockMetaServiceImpl : public cedar::meta::MetaService::Service {
 public:
  grpc::Status GetSpacePartitionMap(
      grpc::ServerContext* /*context*/,
      const cedar::meta::GetSpacePartitionMapRequest* /*request*/,
      cedar::meta::GetSpacePartitionMapResponse* response) override {
    response->set_success(true);
    response->mutable_partition_map()->set_space_name("default");
    response->mutable_partition_map()->set_num_partitions(1);
    return grpc::Status::OK;
  }

  grpc::Status GetAliveNodes(
      grpc::ServerContext* /*context*/,
      const cedar::meta::GetAliveNodesRequest* /*request*/,
      cedar::meta::GetAliveNodesResponse* response) override {
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status RegisterQueryD(
      grpc::ServerContext* /*context*/,
      const cedar::meta::RegisterQueryDRequest* request,
      cedar::meta::RegisterQueryDResponse* response) override {
    last_listen_address = request->listen_address();
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status QueryDHeartbeat(
      grpc::ServerContext* /*context*/,
      const cedar::meta::QueryDHeartbeatRequest* request,
      cedar::meta::QueryDHeartbeatResponse* response) override {
    last_active_queries = request->active_queries();
    last_queued_queries = request->queued_queries();
    response->set_success(true);
    return grpc::Status::OK;
  }

  std::string last_listen_address;
  uint32_t last_active_queries = 0;
  uint32_t last_queued_queries = 0;
};

class MetaRegistrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port_);
    builder.RegisterService(&mock_service_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    ASSERT_GT(port_, 0);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
  }

  std::string GetServerAddress() const {
    return "127.0.0.1:" + std::to_string(port_);
  }

  MockMetaServiceImpl mock_service_;
  std::unique_ptr<grpc::Server> server_;
  int port_ = 0;
};

TEST_F(MetaRegistrationTest, RegisterQueryDSendsAddress) {
  QueryMetaClient::Options options;
  options.meta_service_address = GetServerAddress();
  options.rpc_timeout = std::chrono::milliseconds(5000);
  options.enable_cache = false;
  options.refresh_interval = std::chrono::seconds(1);

  QueryMetaClient client(options);
  // Note: Init() calls FetchSchemaFromMeta and FetchClusterStateFromMeta
  // which are currently stubbed, so we skip Init() and test directly.
  // To test the real gRPC path we manually create a channel.
  auto channel = grpc::CreateChannel(options.meta_service_address,
                                     grpc::InsecureChannelCredentials());
  // Use the internal stub pattern by constructing a client with a channel
  // but we will test via the public API after Init.
  // Since Init may fail on schema fetch (stubbed), we test the methods
  // by calling them directly against a client whose channel we set up.
  
  // Create client and manually inject channel to bypass Init() stub issues
  QueryMetaClient client2(options);
  // We use reflection to set the private channel_, but that's not possible.
  // Instead, we call Init() which creates the channel; the schema fetch
  // stubs return OK so Init() should succeed.
  Status s = client2.Init();
  EXPECT_TRUE(s.ok()) << s.ToString();

  s = client2.RegisterQueryD("127.0.0.1:9669");
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(mock_service_.last_listen_address, "127.0.0.1:9669");
}

TEST_F(MetaRegistrationTest, HeartbeatSendsQueryCounts) {
  QueryMetaClient::Options options;
  options.meta_service_address = GetServerAddress();
  options.rpc_timeout = std::chrono::milliseconds(5000);
  options.enable_cache = false;
  options.refresh_interval = std::chrono::seconds(1);

  QueryMetaClient client(options);
  Status s = client.Init();
  EXPECT_TRUE(s.ok()) << s.ToString();

  s = client.Heartbeat(5, 3);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(mock_service_.last_active_queries, 5u);
  EXPECT_EQ(mock_service_.last_queued_queries, 3u);
}

TEST_F(MetaRegistrationTest, HeartbeatZeroQueries) {
  QueryMetaClient::Options options;
  options.meta_service_address = GetServerAddress();
  options.rpc_timeout = std::chrono::milliseconds(5000);
  options.enable_cache = false;
  options.refresh_interval = std::chrono::seconds(1);

  QueryMetaClient client(options);
  Status s = client.Init();
  EXPECT_TRUE(s.ok()) << s.ToString();

  s = client.Heartbeat(0, 0);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(mock_service_.last_active_queries, 0u);
  EXPECT_EQ(mock_service_.last_queued_queries, 0u);
}
