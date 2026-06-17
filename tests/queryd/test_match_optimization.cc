// Copyright (c) 2025 The Cedar Authors
// Integration tests for MATCH query optimization:
// label index lookup, StorageBackedExecutionContext label filtering, partition pruning.

#include <gtest/gtest.h>
#include <filesystem>
#include <atomic>
#include <unistd.h>

#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/queryd/storage_execution_context.h"
#include "cedar/queryd/query_storage_client.h"
#include "cedar/queryd/distributed_executor.h"
#include "cedar/queryd/meta_client.h"
#include "cedar/types/descriptor.h"

using namespace cedar;
using namespace cedar::queryd;

static std::string GetTempDbPath() {
  static std::atomic<int> counter{0};
  auto pid = getpid();
  auto seq = counter.fetch_add(1);
  auto tmp = std::filesystem::temp_directory_path() /
             ("cedar_match_opt_test_" + std::to_string(pid) + "_" + std::to_string(seq));
  std::filesystem::remove_all(tmp);
  std::filesystem::create_directories(tmp);
  return tmp.string();
}

// ============================================================================
// Mock MetaClient (returns no cluster state)
// ============================================================================

class TestMetaClient : public QueryMetaClient {
 public:
  TestMetaClient() : QueryMetaClient(Options{}) {}
  const ClusterState* GetCachedClusterState() const override { return nullptr; }
  Status GetClusterState(ClusterState* state) override {
    return Status::NotFound("no cluster state in test");
  }
};

// ============================================================================
// LsmEngine label index tests
// ============================================================================

class MatchOptimizationTest : public ::testing::Test {
 protected:
  std::string db_path_;
  std::unique_ptr<LsmEngine> engine_;

  void SetUp() override {
    db_path_ = GetTempDbPath();
    CedarOptions options;
    options.create_if_missing = true;
    engine_ = std::make_unique<LsmEngine>(db_path_, options, nullptr);
    ASSERT_TRUE(engine_->Open().ok());

    InsertEntityWithLabel(1, "Per");
    InsertEntityWithLabel(2, "Per");
    InsertEntityWithLabel(3, "Mov");
  }

  void TearDown() override {
    engine_.reset();
    if (!db_path_.empty() && std::filesystem::exists(db_path_)) {
      std::filesystem::remove_all(db_path_);
    }
  }

  void InsertEntityWithLabel(uint64_t id, const std::string& label) {
    CedarKey key = CedarKey::Vertex(id, LsmEngine::kLabelColumnId, Timestamp::Static());
    auto desc_opt = Descriptor::InlineShortStr(LsmEngine::kLabelColumnId, Slice(label));
    ASSERT_TRUE(desc_opt.has_value()) << "Label too long for inline: " << label;
    ASSERT_TRUE(engine_->Put(key, *desc_opt, Timestamp(1)).ok());
  }
};

TEST_F(MatchOptimizationTest, LabelIndexLookup) {
  auto person_ids = engine_->LookupLabelIndex("Per");
  ASSERT_EQ(person_ids.size(), 2);
  EXPECT_EQ(person_ids[0], 1);
  EXPECT_EQ(person_ids[1], 2);

  auto movie_ids = engine_->LookupLabelIndex("Mov");
  ASSERT_EQ(movie_ids.size(), 1);
  EXPECT_EQ(movie_ids[0], 3);
}

TEST_F(MatchOptimizationTest, LabelIndexNonExistent) {
  auto ids = engine_->LookupLabelIndex("NonExistent");
  EXPECT_TRUE(ids.empty());
}

TEST_F(MatchOptimizationTest, LabelIndexMultipleEntities) {
  InsertEntityWithLabel(4, "Per");
  InsertEntityWithLabel(5, "Per");

  auto person_ids = engine_->LookupLabelIndex("Per");
  ASSERT_EQ(person_ids.size(), 4);
  EXPECT_EQ(person_ids[0], 1);
  EXPECT_EQ(person_ids[1], 2);
  EXPECT_EQ(person_ids[2], 4);
  EXPECT_EQ(person_ids[3], 5);
}

TEST_F(MatchOptimizationTest, LabelIndexRemoveEntity) {
  ASSERT_EQ(engine_->LookupLabelIndex("Per").size(), 2);

  engine_->RemoveFromIndexes(1);

  auto person_ids = engine_->LookupLabelIndex("Per");
  ASSERT_EQ(person_ids.size(), 1);
  EXPECT_EQ(person_ids[0], 2);
}

TEST_F(MatchOptimizationTest, LabelIndexDifferentLabelsIsolated) {
  auto person_ids = engine_->LookupLabelIndex("Per");
  auto movie_ids = engine_->LookupLabelIndex("Mov");

  ASSERT_EQ(person_ids.size(), 2);
  ASSERT_EQ(movie_ids.size(), 1);
  EXPECT_NE(person_ids[0], movie_ids[0]);
}

// ============================================================================
// StorageBackedExecutionContext label filtering tests
// ============================================================================

// NOTE: These tests verify that StorageBackedExecutionContext doesn't crash
// when constructed with various parameters. Full entity-return validation requires
// a running storage server, which is tested in integration tests.

TEST_F(MatchOptimizationTest, StorageBackedContextWithLabel) {
  QueryStorageClient client;

  StorageBackedExecutionContext ctx(&client, 0, "default", "Per");

  ASSERT_NE(ctx.get_all_entities_fn, nullptr);

  auto entities = ctx.get_all_entities_fn(0, 100, 1);
  (void)entities;
}

TEST_F(MatchOptimizationTest, StorageBackedContextWithoutLabel) {
  QueryStorageClient client;

  StorageBackedExecutionContext ctx(&client, 0, "default", "");

  ASSERT_NE(ctx.get_all_entities_fn, nullptr);

  auto entities = ctx.get_all_entities_fn(0, 100, 1);
  (void)entities;
}

TEST_F(MatchOptimizationTest, StorageBackedContextNullClient) {
  StorageBackedExecutionContext ctx(nullptr, 0, "default", "Per");

  ASSERT_NE(ctx.get_all_entities_fn, nullptr);
  auto entities = ctx.get_all_entities_fn(0, 100, 1);
  EXPECT_TRUE(entities.empty());
}

TEST_F(MatchOptimizationTest, StorageBackedContextNullClientNoLabel) {
  StorageBackedExecutionContext ctx(nullptr, 0, "default", "");

  ASSERT_NE(ctx.get_all_entities_fn, nullptr);
  auto entities = ctx.get_all_entities_fn(0, 100, 1);
  EXPECT_TRUE(entities.empty());
}

// ============================================================================
// Partition pruning tests
// ============================================================================

// Must be in cedar::queryd to match the friend declaration in DistributedExecutor
namespace cedar {
namespace queryd {

class TestableDistributedExecutor : public DistributedExecutor {
 public:
  using DistributedExecutor::DistributedExecutor;

  void ExposeUpdateLabelPartitionCache(const std::string& label, uint32_t partition_id) {
    UpdateLabelPartitionCache(label, partition_id);
  }

  std::unordered_set<uint32_t> ExposeGetPartitionsForLabel(const std::string& label) {
    return GetPartitionsForLabel(label);
  }
};

}  // namespace queryd
}  // namespace cedar

TEST(MatchOptimizationPartitionPruning, EmptyCacheReturnsNoPartitions) {
  QueryStorageClient client;
  TestMetaClient meta;
  TestableDistributedExecutor executor(&client, &meta, 1);

  auto partitions = executor.ExposeGetPartitionsForLabel("Per");
  EXPECT_TRUE(partitions.empty());
}

TEST(MatchOptimizationPartitionPruning, CacheReturnsSinglePartition) {
  QueryStorageClient client;
  TestMetaClient meta;
  TestableDistributedExecutor executor(&client, &meta, 1);

  executor.ExposeUpdateLabelPartitionCache("Per", 3);

  auto partitions = executor.ExposeGetPartitionsForLabel("Per");
  ASSERT_EQ(partitions.size(), 1);
  EXPECT_EQ(*partitions.begin(), 3);
}

TEST(MatchOptimizationPartitionPruning, CacheReturnsMultiplePartitions) {
  QueryStorageClient client;
  TestMetaClient meta;
  TestableDistributedExecutor executor(&client, &meta, 1);

  executor.ExposeUpdateLabelPartitionCache("Per", 0);
  executor.ExposeUpdateLabelPartitionCache("Per", 5);
  executor.ExposeUpdateLabelPartitionCache("Per", 7);

  auto partitions = executor.ExposeGetPartitionsForLabel("Per");
  ASSERT_EQ(partitions.size(), 3);
  EXPECT_TRUE(partitions.count(0));
  EXPECT_TRUE(partitions.count(5));
  EXPECT_TRUE(partitions.count(7));
}

TEST(MatchOptimizationPartitionPruning, DifferentLabelsIsolated) {
  QueryStorageClient client;
  TestMetaClient meta;
  TestableDistributedExecutor executor(&client, &meta, 1);

  executor.ExposeUpdateLabelPartitionCache("Per", 1);
  executor.ExposeUpdateLabelPartitionCache("Mov", 2);

  auto per_partitions = executor.ExposeGetPartitionsForLabel("Per");
  auto mov_partitions = executor.ExposeGetPartitionsForLabel("Mov");

  ASSERT_EQ(per_partitions.size(), 1);
  ASSERT_EQ(mov_partitions.size(), 1);
  EXPECT_EQ(*per_partitions.begin(), 1);
  EXPECT_EQ(*mov_partitions.begin(), 2);
}

TEST(MatchOptimizationPartitionPruning, DuplicatePartitionIdDeduplicated) {
  QueryStorageClient client;
  TestMetaClient meta;
  TestableDistributedExecutor executor(&client, &meta, 1);

  executor.ExposeUpdateLabelPartitionCache("Per", 3);
  executor.ExposeUpdateLabelPartitionCache("Per", 3);
  executor.ExposeUpdateLabelPartitionCache("Per", 3);

  auto partitions = executor.ExposeGetPartitionsForLabel("Per");
  ASSERT_EQ(partitions.size(), 1);
  EXPECT_EQ(*partitions.begin(), 3);
}
