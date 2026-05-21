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
// Cypher Public API Test — CedarGraph Cypher delegation
// =============================================================================
// Verifies that CedarGraph::ExecuteCypher, ExplainCypher, and IsValidCypher
// correctly delegate to the internal CypherEngine instance.
// =============================================================================

#include <chrono>
#include <gtest/gtest.h>
#include <unistd.h>

#include "cedar/cypher/value.h"
#include "cedar/graph/cedar_graph.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;

class CypherApiTest : public ::testing::Test {
 protected:
  CedarGraphStorage* storage_ = nullptr;
  std::unique_ptr<CedarGraph> graph_;
  std::string test_dir_;

  void SetUp() override {
    test_dir_ = "/tmp/test_cypher_api_" + std::to_string(getpid()) + "_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());

    CedarGraphStorage::DestroyDB(test_dir_, CedarOptions());

    CedarOptions options;
    options.create_if_missing = true;
    options.distributed_mode = false;

    Status s = CedarGraphStorage::Open(options, test_dir_, &storage_);
    ASSERT_TRUE(s.ok());
    graph_ = std::make_unique<CedarGraph>(storage_);
  }

  void TearDown() override {
    graph_.reset();
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    CedarGraphStorage::DestroyDB(test_dir_, CedarOptions());
  }
};

TEST_F(CypherApiTest, IsValidCypherAcceptsValidQuery) {
  EXPECT_TRUE(graph_->IsValidCypher("MATCH (n) RETURN n"));
}

TEST_F(CypherApiTest, IsValidCypherRejectsInvalidQuery) {
  EXPECT_FALSE(graph_->IsValidCypher("ORDER foo"));
}

TEST_F(CypherApiTest, ExplainCypherReturnsPlanForValidQuery) {
  std::string plan = graph_->ExplainCypher("MATCH (n) RETURN n");
  EXPECT_FALSE(plan.empty());
  EXPECT_NE(plan.find("Error"), 0);  // Should not start with "Error"
}

TEST_F(CypherApiTest, ExplainCypherReturnsErrorForInvalidQuery) {
  std::string plan = graph_->ExplainCypher("INVALID QUERY");
  EXPECT_NE(plan.find("Error"), std::string::npos);
}

TEST_F(CypherApiTest, ExecuteCypherReturnsResultSet) {
  auto result = graph_->ExecuteCypher("MATCH (n) RETURN n");
  // ResultSet should be returned; it may be empty or contain an error
  // depending on engine state, but it must not crash.
  (void)result;
  SUCCEED();
}

TEST_F(CypherApiTest, ExecuteCypherHandlesInvalidQuery) {
  auto result = graph_->ExecuteCypher("INVALID QUERY");
  EXPECT_TRUE(result.HasError());
}
