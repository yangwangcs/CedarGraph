#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <unistd.h>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/cypher/parser.h"
#include "cedar/cypher/planner.h"
#include "cedar/cypher/execution_plan.h"

using namespace cedar;
using namespace cedar::cypher;

class FullPipelineTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  CedarGraphStorage* storage_ = nullptr;

  void SetUp() override {
    data_dir_ = "/tmp/cedar_full_pipeline_" +
                std::to_string(getpid()) + "_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);

    CedarOptions options;
    options.create_if_missing = true;
    options.write_buffer_size = 4 * 1024 * 1024;
    auto status = CedarGraphStorage::Open(options, data_dir_, &storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(storage_, nullptr);
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    std::filesystem::remove_all(data_dir_);
  }
};

TEST_F(FullPipelineTest, CreateSpacePutGetCypher) {
  // === 1. Put: Create a vertex ===
  CedarKey key = CedarKey::Vertex(1, 0, Timestamp::Now());
  Descriptor desc = Descriptor::InlineInt(0, 42);
  Status s = storage_->Put(key.entity_id(), key.timestamp().value(), desc, Timestamp(1));
  ASSERT_TRUE(s.ok()) << "Put failed: " << s.ToString();

  // === 2. Get: Retrieve the vertex ===
  auto result = storage_->Get(key.entity_id(), key.timestamp().value());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInlineInt().value(), 42);

  // === 3. Cypher: Parse a simple query ===
  CypherParser parser("MATCH (n) RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);
  EXPECT_TRUE(parser.GetError().empty());

  // === 4. Cypher: Build execution plan ===
  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  EXPECT_EQ(plan->GetName(), "ProduceResults");

  // === 5. Cypher: Explain plan contains expected operators ===
  auto explain = plan->Explain(0);
  EXPECT_NE(explain.find("ProduceResults"), std::string::npos);
}

TEST_F(FullPipelineTest, PutMultipleAndCypherWhere) {
  // Insert multiple vertices
  for (int i = 0; i < 5; ++i) {
    CedarKey key = CedarKey::Vertex(static_cast<uint64_t>(i), 0, Timestamp::Now());
    Descriptor desc = Descriptor::InlineInt(0, i * 10);
    ASSERT_TRUE(storage_->Put(key.entity_id(), key.timestamp().value(), desc, Timestamp(1)).ok());
  }

  // Parse a query with WHERE
  CypherParser parser("MATCH (n) WHERE n.age > 20 RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);

  auto explain = plan->Explain(0);
  EXPECT_NE(explain.find("IndexScan"), std::string::npos);
  EXPECT_EQ(explain.find("Filter"), std::string::npos);
}

TEST_F(FullPipelineTest, DeleteAndGetMissing) {
  CedarKey key = CedarKey::Vertex(99, 0, Timestamp::Now());
  Descriptor desc = Descriptor::InlineInt(0, 100);
  ASSERT_TRUE(storage_->Put(key.entity_id(), key.timestamp().value(), desc, Timestamp(1)).ok());

  ASSERT_TRUE(storage_->Delete(key.entity_id(), key.timestamp().value(), Timestamp(2)).ok());

  auto result = storage_->Get(key.entity_id(), key.timestamp().value());
  EXPECT_FALSE(result.has_value());
}
