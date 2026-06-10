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
#include <vector>
#include <cstdint>

#include "cedar/cypher/cypher_engine.h"
#include "cedar/cypher/value.h"

using namespace cedar::cypher;

TEST(CypherGcnRouting, ExpandRoutesToGcnCallback) {
  // Create engine without storage - we will use GCN callback
  CypherEngine engine(nullptr);

  // Track which entity IDs were requested
  std::vector<uint64_t> requested_entities;

  // Set up mock GCN callback: return hardcoded neighbors for entity 1
  engine.SetGcnTraversalCallback(
      [&requested_entities](uint64_t entity_id, uint32_t edge_type, uint64_t query_time) -> std::vector<uint64_t> {
        requested_entities.push_back(entity_id);
        if (entity_id == 1) {
          return {100, 200};
        }
        return {};
      });

  // Execute a simple traversal query
  ResultSet result = engine.Execute("MATCH (a)-[:1]->(b) RETURN b");

  // Should not have errors
  EXPECT_FALSE(result.HasError()) << result.error.value_or("unknown error");

  // We expect 2 results: b=100 and b=200 from entity 1
  EXPECT_EQ(result.records.size(), 2);

  // Verify the results came from the GCN callback
  bool found_100 = false;
  bool found_200 = false;
  for (const auto& record : result.records) {
    auto b_val = record.Get("b");
    ASSERT_TRUE(b_val.has_value());
    ASSERT_TRUE(b_val->IsNode());
    uint64_t b_id = b_val->GetNode().id;
    if (b_id == 100) found_100 = true;
    if (b_id == 200) found_200 = true;
  }
  EXPECT_TRUE(found_100);
  EXPECT_TRUE(found_200);

  // Verify the callback was actually invoked for entity 1
  EXPECT_TRUE(std::find(requested_entities.begin(), requested_entities.end(), 1) != requested_entities.end());
}

TEST(CypherGcnRouting, FallbackToStorageWhenNoCallback) {
  // Create engine without storage and without GCN callback
  CypherEngine engine(nullptr);

  // Execute query without any graph or callback
  ResultSet result = engine.Execute("MATCH (a)-[:1]->(b) RETURN b");

  // Should fail because Expand requires a graph or GCN callback
  EXPECT_TRUE(result.HasError());
  EXPECT_NE(result.error.value_or("").find("requires graph or GCN callback"), std::string::npos);
}

TEST(CypherGcnRouting, CallbackReceivesCorrectEdgeType) {
  CypherEngine engine(nullptr);

  uint32_t received_edge_type = 0;
  engine.SetGcnTraversalCallback(
      [&received_edge_type](uint64_t entity_id, uint32_t edge_type, uint64_t query_time) -> std::vector<uint64_t> {
        received_edge_type = edge_type;
        return {};
      });

  ResultSet result = engine.Execute("MATCH (a)-[:42]->(b) RETURN b");

  EXPECT_FALSE(result.HasError());
  EXPECT_EQ(received_edge_type, 42u);
}
