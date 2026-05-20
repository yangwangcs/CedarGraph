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
// Plan Cache Test — Fingerprint-based cache deduplication
// =============================================================================
// Verifies that parameterized queries (queries that differ only in literal
// values) share a single cache entry when keyed by fingerprint.
// =============================================================================

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>

#include "cedar/cypher/cypher_engine.h"
#include "cedar/cypher/value.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar::cypher;

TEST(PlanCacheTest, ParameterizedQueriesShareCacheEntry) {
  CypherEngine engine(nullptr);

  // Execute two queries with different literal values but same structure
  engine.Execute("MATCH (n) WHERE n.id = 1 RETURN n");
  engine.Execute("MATCH (n) WHERE n.id = 2 RETURN n");

  // They should share a single cache entry keyed by fingerprint
  EXPECT_EQ(engine.GetCacheSize(), 1);
}

TEST(PlanCacheTest, DifferentQueriesHaveDifferentEntries) {
  CypherEngine engine(nullptr);

  // Execute two structurally different queries
  engine.Execute("MATCH (n:Person) RETURN n");
  engine.Execute("MATCH (n:Company) RETURN n");

  // They should have separate cache entries
  EXPECT_EQ(engine.GetCacheSize(), 2);
}

TEST(PlanCacheTest, ClearCacheWorks) {
  CypherEngine engine(nullptr);

  engine.Execute("MATCH (n) WHERE n.id = 1 RETURN n");
  EXPECT_EQ(engine.GetCacheSize(), 1);

  engine.ClearCache();
  EXPECT_EQ(engine.GetCacheSize(), 0);
}

TEST(PlanCacheTest, CacheHitAvoidsReparse) {
  CypherEngine engine(nullptr);

  // First execution populates the cache
  engine.Execute("MATCH (n) WHERE n.x = 42 RETURN n");
  EXPECT_EQ(engine.GetCacheSize(), 1);

  // Second execution with different literal should be a cache hit
  engine.Execute("MATCH (n) WHERE n.x = 99 RETURN n");
  EXPECT_EQ(engine.GetCacheSize(), 1);
}

TEST(PlanCacheTest, ParameterizedExecuteOverloadUsesFingerprint) {
  CypherEngine engine(nullptr);

  // Execute parameterized queries with the same structure but different params
  std::map<std::string, Value> params1;
  params1["id"] = Value(1);
  engine.Execute("MATCH (n) WHERE n.id = $id RETURN n", params1);

  std::map<std::string, Value> params2;
  params2["id"] = Value(2);
  engine.Execute("MATCH (n) WHERE n.id = $id RETURN n", params2);

  // They should share a single cache entry (same query string, same fingerprint)
  EXPECT_EQ(engine.GetCacheSize(), 1);
}

TEST(PlanCacheTest, StoragePointerIsWired) {
  std::string db_path = "/tmp/test_plan_cache_storage_" +
                        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  std::filesystem::remove_all(db_path);

  cedar::CedarOptions options;
  options.create_if_missing = true;
  cedar::CedarGraphStorage* storage = nullptr;
  auto status = cedar::CedarGraphStorage::Open(options, db_path, &storage);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_NE(storage, nullptr);

  {
    CypherEngine engine(storage);

    // Execute a simple query to verify storage is wired into ExecutionContext
    // If graph is not set in ExecutionContext, Execute() returns "Invalid execution context"
    auto result = engine.Execute("MATCH (n) RETURN n");
    EXPECT_FALSE(result.HasError()) << result.error.value_or("unknown error");
  }

  delete storage;
  std::filesystem::remove_all(db_path);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
