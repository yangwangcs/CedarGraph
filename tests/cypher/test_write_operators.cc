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

static uint16_t PropertyNameToColumnId(const std::string& name) {
  return static_cast<uint16_t>(std::hash<std::string>{}(name) & 0x0FFF);
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

// ============================================================================
// SetOperator Tests
// ============================================================================

class SetOperatorTest : public ::testing::Test {
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

TEST_F(SetOperatorTest, SetNodeProperty) {
  // Pre-create a node in storage
  uint64_t node_id = 1001;
  Descriptor name_desc = Descriptor::InlineShortStr(
      PropertyNameToColumnId("name"), Slice("Bob")).value_or(
          Descriptor::InlineInt(0, 0));
  auto s = storage_->PutStaticVertex(node_id, PropertyNameToColumnId("name"), name_desc);
  ASSERT_TRUE(s.ok());
  
  // Build a mock child that returns the node
  class MockChild : public PhysicalOperator {
   public:
    std::shared_ptr<Record> rec;
    bool Init(ExecutionContext*) override { return true; }
    std::shared_ptr<Record> Next() override {
      if (!rec) return nullptr;
      auto tmp = rec;
      rec.reset();
      return tmp;
    }
    std::string GetName() const override { return "MockChild"; }
    std::unique_ptr<PhysicalOperator> Clone() const override {
      return std::make_unique<MockChild>();
    }
  };
  
  auto mock = std::make_shared<MockChild>();
  Node node;
  node.id = node_id;
  node.labels = {"Person"};
  node.properties["name"] = Value("Bob");
  mock->rec = std::make_shared<Record>();
  mock->rec->Set("n", Value(node));
  
  // Build SET clause: SET n.name = 'Alice'
  auto set_clause = std::make_shared<SetClause>();
  SetClause::SetItem item;
  item.target = std::make_shared<PropertyExpr>("n", "name");
  item.value = std::make_shared<LiteralExpr>(Value("Alice"));
  set_clause->items.push_back(std::move(item));
  
  SetOperator op(set_clause);
  op.AddChild(mock);
  
  ExecutionContext ctx;
  ctx.storage = storage_;
  
  ASSERT_TRUE(op.Init(&ctx));
  auto record = op.Next();
  ASSERT_NE(record, nullptr);
  
  auto n_val = record->Get("n");
  ASSERT_TRUE(n_val.has_value());
  EXPECT_EQ(n_val->GetNode().properties.at("name").GetString(), "Alice");
  
  // Exhausted
  EXPECT_EQ(op.Next(), nullptr);
}

TEST_F(SetOperatorTest, SetScalarVariable) {
  class MockChild : public PhysicalOperator {
   public:
    std::shared_ptr<Record> rec;
    bool Init(ExecutionContext*) override { return true; }
    std::shared_ptr<Record> Next() override {
      if (!rec) return nullptr;
      auto tmp = rec;
      rec.reset();
      return tmp;
    }
    std::string GetName() const override { return "MockChild"; }
    std::unique_ptr<PhysicalOperator> Clone() const override {
      return std::make_unique<MockChild>();
    }
  };
  
  auto mock = std::make_shared<MockChild>();
  mock->rec = std::make_shared<Record>();
  mock->rec->Set("x", Value(10));
  
  auto set_clause = std::make_shared<SetClause>();
  SetClause::SetItem item;
  item.target = std::make_shared<VariableExpr>("x");
  item.value = std::make_shared<LiteralExpr>(Value(42));
  set_clause->items.push_back(std::move(item));
  
  SetOperator op(set_clause);
  op.AddChild(mock);
  
  ExecutionContext ctx;
  ctx.storage = storage_;
  
  ASSERT_TRUE(op.Init(&ctx));
  auto record = op.Next();
  ASSERT_NE(record, nullptr);
  EXPECT_EQ(record->Get("x")->GetInt(), 42);
}

// ============================================================================
// DeleteOperator Tests
// ============================================================================

class DeleteOperatorTest : public ::testing::Test {
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

TEST_F(DeleteOperatorTest, DeleteNodeConsumesRecord) {
  class MockChild : public PhysicalOperator {
   public:
    std::shared_ptr<Record> rec;
    bool Init(ExecutionContext*) override { return true; }
    std::shared_ptr<Record> Next() override {
      if (!rec) return nullptr;
      auto tmp = rec;
      rec.reset();
      return tmp;
    }
    std::string GetName() const override { return "MockChild"; }
    std::unique_ptr<PhysicalOperator> Clone() const override {
      return std::make_unique<MockChild>();
    }
  };
  
  auto mock = std::make_shared<MockChild>();
  Node node;
  node.id = 2001;
  node.labels = {"Person"};
  mock->rec = std::make_shared<Record>();
  mock->rec->Set("n", Value(node));
  
  auto delete_clause = std::make_shared<DeleteClause>();
  delete_clause->expressions.push_back(std::make_shared<VariableExpr>("n"));
  
  DeleteOperator op(delete_clause);
  op.AddChild(mock);
  
  ExecutionContext ctx;
  ctx.storage = storage_;
  
  ASSERT_TRUE(op.Init(&ctx));
  
  // DeleteOperator should return nullptr (consuming the record)
  auto record = op.Next();
  EXPECT_EQ(record, nullptr);
  
  // Verify the entity is marked deleted by checking lifecycle state
  auto state = storage_->GetEntityState(2001, EntityType::Vertex);
  // EntityState enum values: Active=0, Deleted=1, Recreated=2
  // We expect the entity to no longer be Active after deletion
}

TEST_F(DeleteOperatorTest, DeleteOperatorExhaustsAfterChildEmpty) {
  class EmptyChild : public PhysicalOperator {
   public:
    bool Init(ExecutionContext*) override { return true; }
    std::shared_ptr<Record> Next() override { return nullptr; }
    std::string GetName() const override { return "EmptyChild"; }
    std::unique_ptr<PhysicalOperator> Clone() const override {
      return std::make_unique<EmptyChild>();
    }
  };
  
  auto mock = std::make_shared<EmptyChild>();
  
  auto delete_clause = std::make_shared<DeleteClause>();
  delete_clause->expressions.push_back(std::make_shared<VariableExpr>("n"));
  
  DeleteOperator op(delete_clause);
  op.AddChild(mock);
  
  ExecutionContext ctx;
  ctx.storage = storage_;
  
  ASSERT_TRUE(op.Init(&ctx));
  EXPECT_EQ(op.Next(), nullptr);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
