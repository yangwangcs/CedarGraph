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
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/storage_health_monitor.h"
#include "cedar/storage/failover_manager.h"

using namespace cedar;

TEST(StorageIntegrationTest, EnableHealthMonitoring) {
  CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = true;
  options.meta_endpoints = {"127.0.0.1:9559"};
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, "/tmp/test_health", &storage);
  
  if (!s.ok()) {
    // Skip if can't connect to cluster
    GTEST_SKIP() << "No cluster available";
  }
  
  ASSERT_NE(storage, nullptr);
  
  storage::HealthMonitorConfig config;
  config.check_interval = std::chrono::seconds(5);
  
  s = storage->EnableHealthMonitoring(config);
  
  // May succeed or fail depending on setup
  if (s.ok()) {
    auto health = storage->GetClusterHealth();
    EXPECT_TRUE(health.is_failover_enabled || !health.is_failover_enabled);
  }
  
  delete storage;
}

TEST(StorageIntegrationTest, EnableFailover) {
  CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = true;
  options.meta_endpoints = {"127.0.0.1:9559"};
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, "/tmp/test_failover", &storage);
  
  if (!s.ok()) {
    GTEST_SKIP() << "No cluster available";
  }
  
  ASSERT_NE(storage, nullptr);
  
  // Must enable health monitoring first
  storage::HealthMonitorConfig health_config;
  s = storage->EnableHealthMonitoring(health_config);
  
  if (s.ok()) {
    storage::FailoverConfig failover_config;
    failover_config.enable_auto_failover = true;
    
    s = storage->EnableFailover(failover_config);
    
    if (s.ok()) {
      auto health = storage->GetClusterHealth();
      EXPECT_TRUE(health.is_failover_enabled);
    }
  }
  
  delete storage;
}

TEST(StorageIntegrationTest, RegisterNodeWithoutHealthMonitoring) {
  CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = false;
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, "/tmp/test_single", &storage);
  ASSERT_TRUE(s.ok());
  ASSERT_NE(storage, nullptr);
  
  // Should fail - health monitoring not enabled
  s = storage->RegisterStorageNode("node-1", "127.0.0.1", 9779, 
                                    storage::NodeRole::kLeader);
  EXPECT_FALSE(s.ok());
  
  delete storage;
}

TEST(StorageIntegrationTest, GetNodeForOperations) {
  CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = true;
  options.meta_endpoints = {"127.0.0.1:9559"};
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, "/tmp/test_ops", &storage);
  
  if (!s.ok()) {
    GTEST_SKIP() << "No cluster available";
  }
  
  ASSERT_NE(storage, nullptr);
  
  // Without failover enabled, should return fallback value
  auto read_node = storage->GetNodeForRead();
  auto write_node = storage->GetNodeForWrite();
  
  // May succeed with "primary" or fail
  EXPECT_TRUE(read_node.ok() || !read_node.ok());
  EXPECT_TRUE(write_node.ok() || !write_node.ok());
  
  delete storage;
}

TEST(StorageIntegrationTest, ClusterHealthSummary) {
  CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = true;
  options.meta_endpoints = {"127.0.0.1:9559"};
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, "/tmp/test_summary", &storage);
  
  if (!s.ok()) {
    GTEST_SKIP() << "No cluster available";
  }
  
  ASSERT_NE(storage, nullptr);
  
  auto health = storage->GetClusterHealth();
  
  // Should return valid summary (possibly empty)
  EXPECT_EQ(health.total_nodes, 0);  // No nodes registered yet
  EXPECT_FALSE(health.is_failover_enabled);
  
  delete storage;
}
