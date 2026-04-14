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
#include <filesystem>
#include <cstdlib>
#include <chrono>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/raft/partition_router.h"
#include "cedar/types/descriptor.h"

using namespace cedar;
using namespace cedar::raft;

class PartitionStorageIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create temporary test directory with unique name
    auto tmp = std::filesystem::temp_directory_path();
    test_dir_ = tmp / ("cedar_partition_test_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(test_dir_);
  }
  
  void TearDown() override {
    // Cleanup test directory
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }
  
  std::filesystem::path test_dir_;
};

TEST_F(PartitionStorageIntegrationTest, BasicPutGetWithPartitionRouter) {
  // Open storage in single-node mode
  CedarOptions options;
  options.create_if_missing = true;
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, test_dir_.string(), &storage);
  ASSERT_TRUE(s.ok()) << "Failed to open storage: " << s.ToString();
  ASSERT_NE(storage, nullptr);
  
  // Initialize partition router
  PartitionRouterConfig router_config;
  router_config.default_replica_count = 1;
  router_config.enable_read_from_follower = true;
  
  s = storage->InitializePartitionRouter(router_config);
  ASSERT_TRUE(s.ok()) << "Failed to initialize partition router: " << s.ToString();
  
  // Register local node
  s = storage->RegisterPartitionNode("local-node", "127.0.0.1", 9779, "dc1");
  ASSERT_TRUE(s.ok()) << "Failed to register node: " << s.ToString();
  
  // Create a partition
  std::vector<std::string> replicas = {"local-node"};
  s = storage->CreatePartition(0, replicas);
  ASSERT_TRUE(s.ok()) << "Failed to create partition: " << s.ToString();
  
  // Test PUT operation
  Descriptor desc = Descriptor::InlineInt(0, 42);
  s = storage->Put(12345, 1000000, desc, Timestamp(1));
  ASSERT_TRUE(s.ok()) << "Failed to put: " << s.ToString();
  
  // Test GET operation
  auto result = storage->Get(12345, 1000000);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInlineInt().value(), 42);
  
  // Cleanup
  delete storage;
}

TEST_F(PartitionStorageIntegrationTest, DeleteAsTombstone) {
  CedarOptions options;
  options.create_if_missing = true;
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, test_dir_.string(), &storage);
  ASSERT_TRUE(s.ok());
  
  // Initialize partition router
  PartitionRouterConfig router_config;
  s = storage->InitializePartitionRouter(router_config);
  ASSERT_TRUE(s.ok());
  
  s = storage->RegisterPartitionNode("local-node", "127.0.0.1", 9779);
  ASSERT_TRUE(s.ok());
  
  // Create partition 0 for entity 99999 (99999 % 65535 = 34464, need to create that partition)
  // For simplicity, use entity 0 which maps to partition 0
  s = storage->CreatePartition(0, {"local-node"});
  ASSERT_TRUE(s.ok());
  
  // Put a value to entity 0
  Descriptor desc = Descriptor::InlineInt(0, 100);
  s = storage->Put(0, 2000000, desc, Timestamp(1));
  ASSERT_TRUE(s.ok());
  
  // Verify it exists
  auto result = storage->Get(0, 2000000);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInlineInt().value(), 100);
  
  // Delete (should be tombstone, not physical delete)
  s = storage->Delete(0, 2000000, Timestamp(2));
  ASSERT_TRUE(s.ok());
  
  // Verify it's deleted (Get should return nullopt)
  result = storage->Get(0, 2000000);
  EXPECT_FALSE(result.has_value()) << "Deleted entity should not be found";
  
  delete storage;
}

TEST_F(PartitionStorageIntegrationTest, MultiplePartitions) {
  CedarOptions options;
  options.create_if_missing = true;
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, test_dir_.string(), &storage);
  ASSERT_TRUE(s.ok());
  
  PartitionRouterConfig router_config;
  s = storage->InitializePartitionRouter(router_config);
  ASSERT_TRUE(s.ok());
  
  s = storage->RegisterPartitionNode("node1", "127.0.0.1", 9779);
  ASSERT_TRUE(s.ok());
  
  // Create partitions for entities 0-9 (each entity maps to itself % 65535)
  // So entity i maps to partition i for i < 65535
  for (int i = 0; i < 10; i++) {
    s = storage->CreatePartition(i, {"node1"});
    ASSERT_TRUE(s.ok()) << "Failed to create partition " << i;
  }
  
  // Put data to entities 0-9 (each in its own partition)
  for (int i = 0; i < 10; i++) {
    Descriptor desc = Descriptor::InlineInt(0, i * 10);
    s = storage->Put(i, 1000000, desc, Timestamp(1));
    EXPECT_TRUE(s.ok()) << "Failed to put entity " << i;
  }
  
  // Verify all data
  for (int i = 0; i < 10; i++) {
    auto result = storage->Get(i, 1000000);
    EXPECT_TRUE(result.has_value()) << "Entity " << i << " not found";
    if (result.has_value()) {
      EXPECT_EQ(result->AsInlineInt().value(), i * 10);
    }
  }
  
  // Check partition stats
  auto stats = storage->GetPartitionStats();
  EXPECT_GE(stats.total_partitions, 10);
  
  delete storage;
}

TEST_F(PartitionStorageIntegrationTest, PartitionStats) {
  CedarOptions options;
  options.create_if_missing = true;
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, test_dir_.string(), &storage);
  ASSERT_TRUE(s.ok());
  
  // Initially no partition router
  auto stats = storage->GetPartitionStats();
  EXPECT_EQ(stats.total_partitions, 0);
  
  // Initialize router
  PartitionRouterConfig router_config;
  s = storage->InitializePartitionRouter(router_config);
  ASSERT_TRUE(s.ok());
  
  s = storage->RegisterPartitionNode("node1", "127.0.0.1", 9779);
  ASSERT_TRUE(s.ok());
  
  s = storage->CreatePartition(0, {"node1"});
  ASSERT_TRUE(s.ok());
  
  stats = storage->GetPartitionStats();
  EXPECT_GE(stats.total_partitions, 1);
  EXPECT_GE(stats.total_nodes, 1);
  
  delete storage;
}

TEST_F(PartitionStorageIntegrationTest, PutGetEdgeWithPartitionRouter) {
  CedarOptions options;
  options.create_if_missing = true;
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, test_dir_.string(), &storage);
  ASSERT_TRUE(s.ok());
  
  PartitionRouterConfig router_config;
  s = storage->InitializePartitionRouter(router_config);
  ASSERT_TRUE(s.ok());
  
  s = storage->RegisterPartitionNode("local-node", "127.0.0.1", 9779);
  ASSERT_TRUE(s.ok());
  
  // Create partitions for both source and destination
  s = storage->CreatePartition(0, {"local-node"});
  ASSERT_TRUE(s.ok());
  
  // Put edge
  Descriptor desc = Descriptor::InlineInt(0, 999);
  s = storage->PutEdge(100, 200, 1, Timestamp(1000000), desc, Timestamp(1));
  ASSERT_TRUE(s.ok()) << "Failed to put edge: " << s.ToString();
  
  // Get edge
  auto result = storage->GetEdge(100, 200, 1, Timestamp(1000000));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInlineInt().value(), 999);
  
  delete storage;
}

TEST_F(PartitionStorageIntegrationTest, PartitionRouterRequired) {
  // Test that storage REQUIRES partition router (no backward compatibility)
  CedarOptions options;
  options.create_if_missing = true;
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, test_dir_.string(), &storage);
  ASSERT_TRUE(s.ok());
  
  // Put without partition router should fail
  Descriptor desc = Descriptor::InlineInt(0, 555);
  s = storage->Put(11111, 3000000, desc, Timestamp(1));
  EXPECT_FALSE(s.ok()) << "Put without PartitionRouter should fail";
  EXPECT_NE(s.ToString().find("PartitionRouter"), std::string::npos);
  
  // Get without partition router should return nullopt
  auto result = storage->Get(11111, 3000000);
  EXPECT_FALSE(result.has_value());
  
  // Delete without partition router should fail
  s = storage->Delete(11111, 3000000, Timestamp(2));
  EXPECT_FALSE(s.ok()) << "Delete without PartitionRouter should fail";
  EXPECT_NE(s.ToString().find("PartitionRouter"), std::string::npos);
  
  delete storage;
}
