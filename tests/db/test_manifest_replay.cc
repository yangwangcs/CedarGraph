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
#include <filesystem>

#include "cedar/db/graph_db.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class ManifestReplayTest : public ::testing::Test {
 protected:
  std::string test_dir_;

  void SetUp() override {
    test_dir_ = "/tmp/cedar_manifest_replay_test_" + std::to_string(getpid());
    std::filesystem::remove_all(test_dir_);
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }
};

TEST_F(ManifestReplayTest, RecoverAfterRestart) {
  CedarGraphOptions options;
  options.create_if_missing = true;

  // Phase 1: Create DB, write data, and flush to generate SST + manifest
  {
    CedarGraphDB* db = nullptr;
    Status s = CedarGraphDB::Open(test_dir_, options, &db);
    ASSERT_TRUE(s.ok()) << "Failed to open DB: " << s.ToString();
    ASSERT_NE(db, nullptr);

    CedarKey key(1, EntityType::Vertex, 0, Timestamp(100), 0, 0, 0, 0);
    Descriptor desc = Descriptor::InlineInt(0, 42);
    s = db->Put(key, desc);
    ASSERT_TRUE(s.ok()) << "Failed to put: " << s.ToString();

    s = db->Flush(FlushOptions{});
    ASSERT_TRUE(s.ok()) << "Failed to flush: " << s.ToString();

    delete db;
  }

  // Phase 2: Reopen DB — must replay manifest and recover version state
  {
    CedarGraphDB* db = nullptr;
    Status s = CedarGraphDB::Open(test_dir_, options, &db);
    ASSERT_TRUE(s.ok()) << "Failed to reopen DB: " << s.ToString();
    ASSERT_NE(db, nullptr);

    CedarKey key(1, EntityType::Vertex, 0, Timestamp(100), 0, 0, 0, 0);
    auto result = db->Get(key);
    ASSERT_TRUE(result.has_value()) << "Key not found after replay";
    EXPECT_EQ(result->AsInlineInt().value_or(0), 42);

    // Phase 3: Verify we can continue writing (manifest file is open)
    CedarKey key2(2, EntityType::Vertex, 0, Timestamp(200), 0, 0, 0, 0);
    Descriptor desc2 = Descriptor::InlineInt(0, 99);
    s = db->Put(key2, desc2);
    ASSERT_TRUE(s.ok()) << "Failed to put after replay: " << s.ToString();

    // Read from memtable before flush
    auto result2 = db->Get(key2);
    ASSERT_TRUE(result2.has_value()) << "Key2 not found in memtable after replay";
    EXPECT_EQ(result2->AsInlineInt().value_or(0), 99);

    // Flush again to ensure manifest append works after replay
    s = db->Flush(FlushOptions{});
    ASSERT_TRUE(s.ok()) << "Failed to flush after replay: " << s.ToString();

    delete db;
  }
}

TEST_F(ManifestReplayTest, ManifestFileOpenedForAppend) {
  CedarGraphOptions options;
  options.create_if_missing = true;

  // Create and close DB
  {
    CedarGraphDB* db = nullptr;
    Status s = CedarGraphDB::Open(test_dir_, options, &db);
    ASSERT_TRUE(s.ok());
    ASSERT_NE(db, nullptr);

    CedarKey key(1, EntityType::Vertex, 0, Timestamp(100), 0, 0, 0, 0);
    s = db->Put(key, Descriptor::InlineInt(0, 1));
    ASSERT_TRUE(s.ok());

    s = db->Flush(FlushOptions{});
    ASSERT_TRUE(s.ok());

    delete db;
  }

  // Reopen and do multiple flushes to ensure manifest append works
  {
    CedarGraphDB* db = nullptr;
    Status s = CedarGraphDB::Open(test_dir_, options, &db);
    ASSERT_TRUE(s.ok());
    ASSERT_NE(db, nullptr);

    for (int i = 0; i < 3; ++i) {
      CedarKey key(i + 10, EntityType::Vertex, 0, Timestamp(300 + i), 0, 0, 0, 0);
      s = db->Put(key, Descriptor::InlineInt(0, 100 + i));
      ASSERT_TRUE(s.ok()) << "Put failed at iteration " << i;

      s = db->Flush(FlushOptions{});
      ASSERT_TRUE(s.ok()) << "Flush failed at iteration " << i;
    }

    delete db;
  }
}
