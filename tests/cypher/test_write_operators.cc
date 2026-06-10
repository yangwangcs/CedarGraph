// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include <gtest/gtest.h>
#include <filesystem>
#include <cstdlib>

#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/ast.h"
#include "cedar/cypher/value.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"

using namespace cedar::cypher;
using namespace cedar;

static std::string GetTempDbPath() {
  auto tmp = std::filesystem::temp_directory_path() / "cedar_write_test";
  std::filesystem::remove_all(tmp);
  std::filesystem::create_directories(tmp);
  return tmp.string();
}

class CreateOperatorTest : public ::testing::Test {
 protected:
  CedarGraphStorage* storage_ = nullptr;
  std::string db_path_;
  
  void SetUp() override {
    db_path_ = GetTempDbPath();
    CedarOptions options;
    options.create_if_missing = true;
    auto s = CedarGraphStorage::Open(options, db_path_, &storage_);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }
  
  void TearDown() override {
    delete storage_;
    std::filesystem::remove_all(db_path_);
  }
};

TEST_F(CreateOperatorTest, CreateSingleNode) {
  // Build a CREATE clause: CREATE (n:Person {name: 'Alice', age: 30})
  auto create_clause = std::make_shared<CreateClause>();
  PathPattern pattern;
  NodePattern node;
  node.variable = "n";
  node.labels = {"Person"};
  node.properties["name"] = std::make_shared<LiteralExpr>(Value("Alice"));
  node.properties["age"] = std::make_shared<LiteralExpr>(Value(30));
  pattern.elements.push_back(node);
  create_clause->patterns.push_back(std::move(pattern));
  
  CreateOperator op(create_clause);
  
  ExecutionContext ctx;
  ctx.storage = storage_;
  
  ASSERT_TRUE(op.Init(&ctx));
  
  auto record = op.Next();
  ASSERT_NE(record, nullptr);
  
  auto n_val = record->Get("n");
  ASSERT_TRUE(n_val.has_value());
  ASSERT_TRUE(n_val->IsNode());
  
  const Node& created = n_val->GetNode();
  EXPECT_EQ(created.labels.size(), 1);
  EXPECT_EQ(created.labels[0], "Person");
  EXPECT_EQ(created.properties.at("name").GetString(), "Alice");
  EXPECT_EQ(created.properties.at("age").GetInt(), 30);
  
  // Next call should return nullptr (CREATE is exhausted)
  EXPECT_EQ(op.Next(), nullptr);
}

TEST_F(CreateOperatorTest, CreateNodeWithoutProperties) {
  auto create_clause = std::make_shared<CreateClause>();
  PathPattern pattern;
  NodePattern node;
  node.variable = "n";
  pattern.elements.push_back(node);
  create_clause->patterns.push_back(std::move(pattern));
  
  CreateOperator op(create_clause);
  
  ExecutionContext ctx;
  ctx.storage = storage_;
  
  ASSERT_TRUE(op.Init(&ctx));
  auto record = op.Next();
  ASSERT_NE(record, nullptr);
  EXPECT_TRUE(record->Get("n")->IsNode());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
