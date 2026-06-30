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

#include <gtest/gtest.h>
#include "cedar/dtx/service_discovery.h"
#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/meta_service_grpc.h"
#include "test_tls_certs.h"

using namespace cedar;
using namespace cedar::dtx;

class ClusterInitializerRegistrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(cedar::test::SetupTestTlsEnvironment("cluster_initializer"));

    MetaServiceConfig config;
    config.node_id = 1;
    config.listen_address = "localhost:19560";
    config.advertise_address = "localhost:19560";
    config.test_mode = true;

    auto status = meta_service_.Initialize(config);
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto grpc_status = grpc_server_.Start("localhost:19560", &meta_service_);
    ASSERT_TRUE(grpc_status.ok()) << grpc_status.ToString();
  }

  void TearDown() override {
    grpc_server_.Stop();
    meta_service_.Shutdown();
  }

  MetadataService meta_service_;
  MetaServiceGrpcServer grpc_server_;
};

TEST_F(ClusterInitializerRegistrationTest, RegisterStorageNodesViaRealRpc) {
  ClusterInitializer::Config init_config;
  init_config.meta_servers = {"localhost:19560"};
  init_config.auto_discover_storaged = false;

  ClusterInitializer initializer(init_config);

  // Manually construct some storage nodes to register
  std::vector<StorageNodeInfo> nodes;
  StorageNodeInfo n1;
  n1.host = "10.0.0.1";
  n1.port = 9779;
  n1.ip_address = "10.0.0.1";
  n1.is_healthy = true;
  nodes.push_back(n1);

  StorageNodeInfo n2;
  n2.host = "10.0.0.2";
  n2.port = 9779;
  n2.ip_address = "10.0.0.2";
  n2.is_healthy = true;
  nodes.push_back(n2);

  auto status = initializer.RegisterStorageNodes(nodes);
  EXPECT_TRUE(status.ok()) << status.ToString();

  // Verify nodes were actually registered by querying MetaService
  auto alive = meta_service_.GetAliveNodes();
  EXPECT_EQ(alive.size(), 2u) << "Expected 2 nodes registered in MetaService";
}

TEST_F(ClusterInitializerRegistrationTest, RegisterEmptyNodesReturnsError) {
  ClusterInitializer::Config init_config;
  init_config.meta_servers = {"localhost:19560"};
  init_config.auto_discover_storaged = false;

  ClusterInitializer initializer(init_config);

  std::vector<StorageNodeInfo> nodes;
  auto status = initializer.RegisterStorageNodes(nodes);
  EXPECT_FALSE(status.ok()) << "Expected error for empty node list";
}

TEST(ClusterInitializerConfigTest, WaitForMetaDRejectsEmptyServerList) {
  ClusterInitializer::Config init_config;
  init_config.meta_servers = {};
  init_config.init_timeout_seconds = 1;
  init_config.retry_interval_seconds = 1;

  ClusterInitializer initializer(init_config);
  auto status = initializer.WaitForMetaD();

  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}

TEST(ClusterInitializerConfigTest, WaitForMetaDRejectsNonPositiveIntervals) {
  ClusterInitializer::Config init_config;
  init_config.meta_servers = {"127.0.0.1:1"};
  init_config.init_timeout_seconds = 0;
  init_config.retry_interval_seconds = 1;

  ClusterInitializer invalid_timeout(init_config);
  EXPECT_TRUE(invalid_timeout.WaitForMetaD().IsInvalidArgument());

  init_config.init_timeout_seconds = 1;
  init_config.retry_interval_seconds = 0;
  ClusterInitializer invalid_retry(init_config);
  EXPECT_TRUE(invalid_retry.WaitForMetaD().IsInvalidArgument());
}

TEST(ClusterInitializerConfigTest, WaitForMetaDSkipsInvalidEndpointsAndTimesOut) {
  ClusterInitializer::Config init_config;
  init_config.meta_servers = {"localhost:not-a-port", "127.0.0.1:1"};
  init_config.init_timeout_seconds = 1;
  init_config.retry_interval_seconds = 1;

  ClusterInitializer initializer(init_config);

  auto start = std::chrono::steady_clock::now();
  auto status = initializer.WaitForMetaD();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_FALSE(status.ok());
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            2000);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
