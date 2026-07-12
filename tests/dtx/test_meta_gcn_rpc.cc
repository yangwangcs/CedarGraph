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

#include <chrono>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/meta_service_grpc.h"
#include "cedar/gcn/coordinator_client.h"

namespace cedar {
namespace dtx {
namespace {

uint64_t NowMs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

class MetaGcnRpcTest : public ::testing::Test {
 protected:
  void SetUp() override {
    MetaServiceConfig config;
    config.node_id = 1;
    config.listen_address = "127.0.0.1:2379";
    config.advertise_address = "127.0.0.1:2379";
    config.test_mode = true;
    ASSERT_TRUE(meta_.Initialize(config).ok());

    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port_);
    service_ = std::make_unique<MetaServiceGrpcImpl>(&meta_);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);

    std::ostringstream address;
    address << "127.0.0.1:" << port_;
    address_ = address.str();
    client_ = std::make_unique<gcn::CoordinatorClient>(
        grpc::CreateChannel(address_, grpc::InsecureChannelCredentials()));
    server_thread_ = std::thread([this]() { server_->Wait(); });
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
    }
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
    service_.reset();
    ASSERT_TRUE(meta_.Shutdown().ok());
  }

  MetadataService meta_;
  std::unique_ptr<MetaServiceGrpcImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  std::thread server_thread_;
  std::unique_ptr<gcn::CoordinatorClient> client_;
  std::string address_;
  int port_ = 0;
};

class RedirectingMetaService final : public cedar::meta::MetaService::Service {
 public:
  explicit RedirectingMetaService(std::string leader_address)
      : leader_address_(std::move(leader_address)) {}

  grpc::Status RegisterGcn(grpc::ServerContext*,
                           const cedar::meta::RegisterGcnRequest*,
                           cedar::meta::RegisterGcnResponse* response) override {
    ++register_calls;
    response->set_success(false);
    response->set_status_code(cedar::meta::GCN_LEASE_STATUS_NOT_LEADER);
    response->set_error_msg("Not a leader");
    response->set_leader_address(leader_address_);
    return grpc::Status::OK;
  }

  grpc::Status RenewGcnLeases(
      grpc::ServerContext*,
      const cedar::meta::RenewGcnLeasesRequest*,
      cedar::meta::RenewGcnLeasesResponse* response) override {
    ++renew_calls;
    response->set_success(false);
    response->set_status_code(cedar::meta::GCN_LEASE_STATUS_NOT_LEADER);
    response->set_error_msg("Not a leader");
    response->set_leader_address(leader_address_);
    return grpc::Status::OK;
  }

  int register_calls = 0;
  int renew_calls = 0;

 private:
  std::string leader_address_;
};

TEST_F(MetaGcnRpcTest, RegistrationAndRenewalReturnLeaseToken) {
  auto status = client_->RegisterGcn(7, "gcn-7:9780", 42);
  ASSERT_TRUE(status.ok()) << status.ToString();

  auto leases = client_->RenewGcnLeases(7, 42, {});
  ASSERT_TRUE(leases.ok()) << leases.status().ToString();
  ASSERT_FALSE(leases.ValueOrDie().empty());
  EXPECT_GT(leases.ValueOrDie().front().lease_epoch, 0u);
  EXPECT_GT(leases.ValueOrDie().front().expires_at_ms, NowMs());
  EXPECT_FALSE(leases.ValueOrDie().front().lease_token.empty());
}

TEST_F(MetaGcnRpcTest, OldIncarnationCannotRenew) {
  auto status = client_->RegisterGcn(7, "gcn-7:9780", 43);
  ASSERT_TRUE(status.ok()) << status.ToString();

  auto leases = client_->RenewGcnLeases(7, 42, {});
  ASSERT_FALSE(leases.ok());
  EXPECT_TRUE(leases.status().IsConflict()) << leases.status().ToString();
}

TEST_F(MetaGcnRpcTest, LocateReturnsEligibleGcnRoute) {
  ASSERT_TRUE(client_->RegisterGcn(7, "gcn-7:9780", 42).ok());

  auto leases = client_->RenewGcnLeases(
      7, 42, {GcnPartitionProgress{/*partition_id=*/3,
                                   /*partition_epoch=*/1,
                                   /*applied_offset=*/123,
                                   /*applied_version=*/99,
                                   /*query_ready=*/true}});
  ASSERT_TRUE(leases.ok()) << leases.status().ToString();

  auto route = client_->LocateGcn(3, 99);
  ASSERT_TRUE(route.ok()) << route.status().ToString();
  EXPECT_EQ(route.ValueOrDie().partition_id, 3u);
  EXPECT_EQ(route.ValueOrDie().gcn_id, 7u);
  EXPECT_EQ(route.ValueOrDie().endpoint, "gcn-7:9780");
  EXPECT_EQ(route.ValueOrDie().applied_version, 99u);
}

TEST_F(MetaGcnRpcTest, StalePartitionEpochReturnsStaleLeaseStatus) {
  ASSERT_TRUE(client_->RegisterGcn(7, "gcn-7:9780", 42).ok());

  auto leases = client_->RenewGcnLeases(7, 42, {});
  ASSERT_TRUE(leases.ok()) << leases.status().ToString();
  const auto epoch = leases.ValueOrDie().front().lease_epoch;

  auto stale = client_->RenewGcnLeases(
      7, 42, {GcnPartitionProgress{leases.ValueOrDie().front().partition_id,
                                   epoch + 1,
                                   123,
                                   99,
                                   true}});
  ASSERT_FALSE(stale.ok());
  EXPECT_TRUE(stale.status().IsConflict()) << stale.status().ToString();
}

TEST_F(MetaGcnRpcTest, RegisterAndRenewRetryNotLeaderRedirectOnce) {
  RedirectingMetaService redirect_service(address_);
  int redirect_port = 0;
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &redirect_port);
  builder.RegisterService(&redirect_service);
  auto redirect_server = builder.BuildAndStart();
  ASSERT_NE(redirect_server, nullptr);
  std::thread redirect_thread([&]() { redirect_server->Wait(); });

  std::ostringstream redirect_address;
  redirect_address << "127.0.0.1:" << redirect_port;
  gcn::CoordinatorClient redirected_client(grpc::CreateChannel(
      redirect_address.str(), grpc::InsecureChannelCredentials()));

  auto status = redirected_client.RegisterGcn(8, "gcn-8:9780", 50);
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(redirect_service.register_calls, 1);

  auto leases = redirected_client.RenewGcnLeases(8, 50, {});
  EXPECT_TRUE(leases.ok()) << leases.status().ToString();
  EXPECT_FALSE(leases.ValueOrDie().empty());
  EXPECT_EQ(redirect_service.renew_calls, 1);

  redirect_server->Shutdown();
  redirect_thread.join();
}

TEST_F(MetaGcnRpcTest, LocateWithoutEligibleGcnReturnsNotFound) {
  auto route = client_->LocateGcn(3, 99);
  ASSERT_FALSE(route.ok());
  EXPECT_TRUE(route.status().IsNotFound()) << route.status().ToString();
}

}  // namespace
}  // namespace dtx
}  // namespace cedar
