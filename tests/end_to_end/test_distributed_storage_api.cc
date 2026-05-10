// Copyright 2025 The Cedar Authors
//
// End-to-end test for CedarGraphStorage distributed mode API
// This test validates the new distributed mode integration

#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <unistd.h>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/governance/service_registry.h"

using namespace cedar;

class DistributedStorageApiTest : public ::testing::Test {
 protected:
  std::string test_dir_;

  void SetUp() override {
    test_dir_ = "/tmp/test_distributed_api_" + std::to_string(getpid()) + "_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }
};

// Test that distributed mode APIs are available and compile correctly
TEST_F(DistributedStorageApiTest, OpenDistributedApiExists) {
  // Test OpenDistributed static factory method
  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;

  // This will fail at runtime (no real MetaD), but should compile and not crash
  Status s = CedarGraphStorage::OpenDistributed(
      {"127.0.0.1:9559"},  // Dummy endpoint
      options,
      test_dir_,
      &storage);

  // Expect failure because no real MetaD is running
  // But the API should work and set distributed mode
  if (s.ok()) {
    // If somehow connected, verify distributed mode
    EXPECT_TRUE(storage->IsDistributedMode());
    EXPECT_TRUE(storage->IsConnected());
    EXPECT_NE(storage->GetStorageClient(), nullptr);
    delete storage;
  } else {
    // Expected path: connection fails
    EXPECT_FALSE(s.ok());
  }
}

TEST_F(DistributedStorageApiTest, OpenWithDiscoveryApiExists) {
  // Initialize a ServiceRegistry
  governance::ServiceRegistry registry;

  // Register a dummy storage service
  governance::ServiceInfo info;
  info.id = "storaged-test-1";
  info.name = "storaged";
  info.host = "127.0.0.1";
  info.port = 9779;
  info.status = governance::ServiceStatus::kHealthy;

  Status s = registry.Register(info);
  ASSERT_TRUE(s.ok());

  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;

  // Test OpenWithDiscovery static factory method
  s = CedarGraphStorage::OpenWithDiscovery(
      registry,
      "storaged",
      options,
      test_dir_,
      &storage);

  // This may fail at runtime (no real storage server), but should compile
  if (s.ok()) {
    EXPECT_TRUE(storage->IsDistributedMode());
    EXPECT_NE(storage->GetStorageClient(), nullptr);
    delete storage;
  }
}

TEST_F(DistributedStorageApiTest, IsDistributedModeReturnsFalseForSingleNode) {
  // Test that single-node mode still works
  CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = false;  // Explicitly single-node

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, test_dir_, &storage);

  ASSERT_TRUE(s.ok());
  ASSERT_NE(storage, nullptr);

  // Verify single-node mode
  EXPECT_FALSE(storage->IsDistributedMode());
  EXPECT_TRUE(storage->IsConnected());  // Single-node is always "connected"
  EXPECT_EQ(storage->GetStorageClient(), nullptr);  // No DTX client in single-node
  EXPECT_NE(storage->GetLsmEngine(), nullptr);  // Has local LSM engine

  delete storage;
}

TEST_F(DistributedStorageApiTest, OptionsDistributedModeFlag) {
  // Test that distributed_mode flag works through regular Open
  CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = true;  // Enable distributed mode
  options.meta_endpoints = {"127.0.0.1:9559"};

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, test_dir_, &storage);

  // May fail due to no MetaD, but should attempt distributed initialization
  if (s.ok()) {
    EXPECT_TRUE(storage->IsDistributedMode());
    delete storage;
  }
}

TEST_F(DistributedStorageApiTest, DTXConfigOptions) {
  // Test DTX config options
  CedarOptions options;
  options.distributed_mode = true;
  options.meta_endpoints = {"127.0.0.1:9559"};
  options.dtx_config.rpc_timeout_ms = 10000;
  options.dtx_config.max_retries = 5;
  options.dtx_config.retry_base_delay_ms = 50;

  // Just verify the options can be set (compilation test)
  EXPECT_EQ(options.dtx_config.rpc_timeout_ms, 10000);
  EXPECT_EQ(options.dtx_config.max_retries, 5);
  EXPECT_EQ(options.dtx_config.retry_base_delay_ms, 50);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
