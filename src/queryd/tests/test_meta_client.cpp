// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Tests for Meta Client

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "cedar/queryd/meta_client.h"

using namespace cedar::queryd;

class SchemaCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cache_ = std::make_unique<SchemaCache>(100);
  }
  
  std::unique_ptr<SchemaCache> cache_;
};

TEST_F(SchemaCacheTest, BasicOperations) {
  // Cache plan
  cache_->CachePlan("MATCH (n:Person) RETURN n", "SCAN Node[Person]");
  
  // Get plan
  std::string plan = cache_->GetPlan("MATCH (n:Person) RETURN n");
  EXPECT_EQ(plan, "SCAN Node[Person]");
  
  // Get non-existent
  plan = cache_->GetPlan("MATCH (n:Company) RETURN n");
  EXPECT_TRUE(plan.empty());
}

TEST_F(SchemaCacheTest, InvalidateAll) {
  cache_->CachePlan("query1", "plan1");
  cache_->CachePlan("query2", "plan2");
  
  EXPECT_EQ(cache_->Size(), 2);
  
  cache_->InvalidateAll();
  
  EXPECT_EQ(cache_->Size(), 0);
  EXPECT_TRUE(cache_->GetPlan("query1").empty());
}

TEST_F(SchemaCacheTest, Eviction) {
  SchemaCache small_cache(5);
  
  // Add more entries than max
  for (int i = 0; i < 10; ++i) {
    small_cache.CachePlan("query" + std::to_string(i), 
                          "plan" + std::to_string(i));
  }
  
  // Some entries should have been evicted
  EXPECT_LE(small_cache.Size(), 5);
}

class GraphSchemaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Setup test schema
    LabelSchema person;
    person.name = "Person";
    person.is_node = true;
    person.properties.push_back({"id", "STRING", false, true, ""});
    person.properties.push_back({"name", "STRING", false, false, ""});
    person.properties.push_back({"age", "INT", true, false, "0"});
    
    LabelSchema company;
    company.name = "Company";
    company.is_node = true;
    company.properties.push_back({"id", "STRING", false, true, ""});
    company.properties.push_back({"name", "STRING", false, false, ""});
    
    LabelSchema works_for;
    works_for.name = "WORKS_FOR";
    works_for.is_node = false;
    works_for.properties.push_back({"since", "STRING", true, false, ""});
    
    schema_.node_labels["Person"] = std::move(person);
    schema_.node_labels["Company"] = std::move(company);
    schema_.edge_types["WORKS_FOR"] = std::move(works_for);
  }
  
  GraphSchema schema_;
};

TEST_F(GraphSchemaTest, GetNodeLabel) {
  auto* person = schema_.GetNodeLabel("Person");
  EXPECT_NE(person, nullptr);
  EXPECT_EQ(person->name, "Person");
  EXPECT_EQ(person->properties.size(), 3);
  
  auto* not_found = schema_.GetNodeLabel("NotExist");
  EXPECT_EQ(not_found, nullptr);
}

TEST_F(GraphSchemaTest, GetEdgeType) {
  auto* works_for = schema_.GetEdgeType("WORKS_FOR");
  EXPECT_NE(works_for, nullptr);
  EXPECT_EQ(works_for->name, "WORKS_FOR");
  EXPECT_FALSE(works_for->is_node);
  
  auto* not_found = schema_.GetEdgeType("NOT_EXIST");
  EXPECT_EQ(not_found, nullptr);
}

class ClusterStateTest : public ::testing::Test {};

TEST_F(ClusterStateTest, GetPartition) {
  ClusterState state;
  state.version = 1;
  state.partition_count = 4;
  
  PartitionInfo p0{0, "127.0.0.1:9779", {}, 0, 0, true, 0, 0};
  PartitionInfo p1{1, "127.0.0.1:9780", {}, 0, 0, true, 0, 0};
  state.partitions.push_back(p0);
  state.partitions.push_back(p1);
  
  auto* partition = state.GetPartition(0);
  EXPECT_NE(partition, nullptr);
  EXPECT_EQ(partition->partition_id, 0);
  
  auto* not_found = state.GetPartition(999);
  EXPECT_EQ(not_found, nullptr);
}

TEST_F(ClusterStateTest, GetPartitionForEntity) {
  ClusterState state;
  state.partition_count = 4;
  
  EXPECT_EQ(state.GetPartitionForEntity(0), 0);
  EXPECT_EQ(state.GetPartitionForEntity(1), 1);
  EXPECT_EQ(state.GetPartitionForEntity(2), 2);
  EXPECT_EQ(state.GetPartitionForEntity(3), 3);
  EXPECT_EQ(state.GetPartitionForEntity(4), 0);
  EXPECT_EQ(state.GetPartitionForEntity(5), 1);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
