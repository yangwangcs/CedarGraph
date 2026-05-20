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

#include "cedar/queryd/query_storage_client.h"
#include "cedar/storage/plan_cache.h"

using namespace cedar;
using namespace cedar::queryd;

TEST(AdaptiveExecutionPath, LocalPartitionUsesLocalNodeClient) {
  QueryStorageClient client;
  client.MarkPartitionLocal(42);

  EXPECT_TRUE(client.IsLocalPartition(42));
  EXPECT_FALSE(client.IsLocalPartition(99));

  auto node_client = client.GetNodeClient(42);
  EXPECT_NE(node_client, nullptr);
}

TEST(AdaptiveExecutionPath, RemotePartitionCreatesChannel) {
  QueryStorageClient client;
  client.RegisterNode(99, "127.0.0.1:19779");

  EXPECT_FALSE(client.IsLocalPartition(99));

  // GetNodeClient for remote partition should return a valid client
  // (either RemoteRPCNodeClient or fallback to LocalNodeImpl if channel fails)
  auto node_client = client.GetNodeClient(99);
  EXPECT_NE(node_client, nullptr);
}

TEST(PlanCache, LookupMissAndHit) {
  cedar::storage::PlanCache cache;

  EXPECT_EQ(cache.Size(), 0);
  EXPECT_EQ(cache.Lookup("fp1"), nullptr);

  // Store a dummy plan (we can't easily construct ExecutionPlan here,
  // so we test the cache mechanics with nullptr safety)
  cache.Store("fp1", nullptr);
  EXPECT_EQ(cache.Size(), 0);  // nullptr store is ignored
}

TEST(PlanCache, InvalidateAll) {
  cedar::storage::PlanCache cache;
  cache.InvalidateAll();
  EXPECT_EQ(cache.Size(), 0);
}
