// Copyright (c) 2025 The Cedar Authors
// LsmEngine secondary index tests

#include <gtest/gtest.h>
#include <filesystem>
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class LsmEngineIndexTest : public ::testing::Test {
 protected:
  std::string db_path_;

  void SetUp() override {
    char buf[] = "/tmp/cedar_index_test_XXXXXX";
    char* dir = mkdtemp(buf);
    ASSERT_NE(dir, nullptr);
    db_path_ = dir;
  }

  void TearDown() override {
    if (!db_path_.empty() && std::filesystem::exists(db_path_)) {
      std::filesystem::remove_all(db_path_);
    }
  }
};

TEST_F(LsmEngineIndexTest, PropertyIndexMaintainedOnPut) {
  CedarOptions options;
  options.create_if_missing = true;

  LsmEngine engine(db_path_, options, nullptr);
  ASSERT_TRUE(engine.Open().ok());

  CedarKey key = CedarKey::Vertex(100, 1, Timestamp::Static());
  Descriptor desc = Descriptor::InlineInt(1, 42);
  ASSERT_TRUE(engine.Put(key, desc, Timestamp(1)).ok());

  auto ids = engine.LookupPropertyIndex(1, "42");
  ASSERT_EQ(ids.size(), 1);
  EXPECT_EQ(ids[0], 100);
}

TEST_F(LsmEngineIndexTest, LabelIndexMaintainedOnPut) {
  CedarOptions options;
  options.create_if_missing = true;

  LsmEngine engine(db_path_, options, nullptr);
  ASSERT_TRUE(engine.Open().ok());

  CedarKey key = CedarKey::Vertex(200, LsmEngine::kLabelColumnId, Timestamp::Static());
  auto desc_opt = Descriptor::InlineShortStr(LsmEngine::kLabelColumnId, Slice("Per"));
  ASSERT_TRUE(desc_opt.has_value());
  ASSERT_TRUE(engine.Put(key, *desc_opt, Timestamp(1)).ok());

  auto ids = engine.LookupLabelIndex("Per");
  ASSERT_EQ(ids.size(), 1);
  EXPECT_EQ(ids[0], 200);
}

TEST_F(LsmEngineIndexTest, IndexRemovesOnDelete) {
  CedarOptions options;
  options.create_if_missing = true;

  LsmEngine engine(db_path_, options, nullptr);
  ASSERT_TRUE(engine.Open().ok());

  CedarKey key = CedarKey::Vertex(300, 2, Timestamp::Static());
  Descriptor desc = Descriptor::InlineInt(2, 99);
  ASSERT_TRUE(engine.Put(key, desc, Timestamp(1)).ok());
  ASSERT_EQ(engine.LookupPropertyIndex(2, "99").size(), 1);

  ASSERT_TRUE(engine.Delete(key, Timestamp(2)).ok());
  EXPECT_EQ(engine.LookupPropertyIndex(2, "99").size(), 0);
}

TEST_F(LsmEngineIndexTest, DuplicateEntityIdNotDuplicated) {
  CedarOptions options;
  options.create_if_missing = true;

  LsmEngine engine(db_path_, options, nullptr);
  ASSERT_TRUE(engine.Open().ok());

  CedarKey key = CedarKey::Vertex(400, 3, Timestamp::Static());
  Descriptor desc = Descriptor::InlineInt(3, 7);
  ASSERT_TRUE(engine.Put(key, desc, Timestamp(1)).ok());
  ASSERT_TRUE(engine.Put(key, desc, Timestamp(2)).ok());

  auto ids = engine.LookupPropertyIndex(3, "7");
  EXPECT_EQ(ids.size(), 1);
  EXPECT_EQ(ids[0], 400);
}
