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

#include "cedar/cypher/parser.h"

using namespace cedar::cypher;

TEST(CypherParser, ParseNodeWithProperties) {
  CypherParser parser("MATCH (n {id: 123, name: 'test'}) RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  auto* match = static_cast<MatchClause*>(stmt->clauses[0].get());
  ASSERT_FALSE(match->patterns.empty());
  ASSERT_FALSE(match->patterns[0].elements.empty());
  auto* node = std::get_if<NodePattern>(&match->patterns[0].elements[0]);
  ASSERT_NE(node, nullptr);
  ASSERT_EQ(node->properties.size(), 2);
  EXPECT_TRUE(node->properties.count("id"));
  EXPECT_TRUE(node->properties.count("name"));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
