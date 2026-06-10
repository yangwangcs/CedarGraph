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

#include "cedar/gcn/coordinator_client.h"
#include "meta_service.grpc.pb.h"

using namespace cedar::gcn;

class MockMetaService final : public cedar::meta::MetaService::Service {
 public:
  grpc::Status LocateCache(grpc::ServerContext* context,
                           const cedar::meta::LocateCacheRequest* request,
                           cedar::meta::LocateCacheResponse* response) override {
    (void)context;
    (void)request;
    response->set_found(true);
    auto* w = response->mutable_window();
    w->set_entity_id(42);
    w->set_cached_from(0);
    w->set_cached_to(1000);
    w->set_gcn_node_id(7);
    w->set_version(3);
    w->set_expire_at(2000);
    return grpc::Status::OK;
  }
};

class CoordinatorClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port_);
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);

    std::ostringstream oss;
    oss << "127.0.0.1:" << port_;
    address_ = oss.str();

    server_thread_ = std::thread([this]() { server_->Wait(); });
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
    }
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
  }

  MockMetaService service_;
  std::unique_ptr<grpc::Server> server_;
  std::thread server_thread_;
  std::string address_;
  int port_ = 0;
};

TEST_F(CoordinatorClientTest, LocateReturnsWindow) {
  auto channel = grpc::CreateChannel(address_, grpc::InsecureChannelCredentials());
  CoordinatorClient client(channel);

  auto result = client.Locate(42, 1000);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->gcn_node_id, 7u);
  EXPECT_EQ(result->version, 3u);
}
