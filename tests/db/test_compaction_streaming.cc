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
#include <vector>

#include "cedar/db/graph_db.h"
#include "cedar/db/graph_db_impl.h"
#include "cedar/sst/zone_columnar_reader.h"
#include "cedar/sst/sst_builder_factory.h"
#include "cedar/core/env.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class CompactionStreamingTest : public ::testing::Test {
 protected:
  std::string test_dir_;
  CedarGraphDB* db_ = nullptr;
  Env* env_ = nullptr;

  void SetUp() override {
    test_dir_ = "/tmp/cedar_compaction_streaming_test_" + std::to_string(getpid());
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
    env_ = Env::Default();

    CedarGraphOptions options;
    options.create_if_missing = true;

    Status s = CedarGraphDB::Open(test_dir_, options, &db_);
    ASSERT_TRUE(s.ok()) << "Failed to open DB: " << s.ToString();
    ASSERT_NE(db_, nullptr);
  }

  void TearDown() override {
    delete db_;
    std::filesystem::remove_all(test_dir_);
  }

  // Create an SST file with specific entries
  Status CreateSstFileWithEntries(
      const std::string& path,
      const std::vector<std::pair<CedarKey, Descriptor>>& entries,
      uint64_t file_number,
      int level) {
    WritableFile* file = nullptr;
    Status s = env_->NewWritableFile(path, &file);
    if (!s.ok()) return s;

    auto builder = SstBuilderFactory::Create(file, test_dir_);
    for (const auto& [key, desc] : entries) {
      builder->Add(key, desc);
    }

    s = builder->Finish();
    delete file;
    if (!s.ok()) {
      env_->RemoveFile(path);
      return s;
    }
    return Status::OK();
  }

  // Register file in VersionSet
  Status RegisterFileInVersionSet(uint64_t file_number,
                                  int level,
                                  uint64_t min_entity,
                                  uint64_t max_entity,
                                  uint64_t min_ts,
                                  uint64_t max_ts,
                                  uint64_t file_size,
                                  uint64_t num_entries) {
    auto* impl = db_->GetInternalImpl();
    auto* vs = impl->GetVersionSet();

    FileMetaData meta;
    meta.file_number = file_number;
    meta.level = level;
    meta.file_size = file_size;
    meta.smallest_entity_id = min_entity;
    meta.largest_entity_id = max_entity;
    meta.smallest_timestamp = min_ts;
    meta.largest_timestamp = max_ts;
    meta.num_entries = num_entries;

    return vs->ApplyEdit(ManifestEdit::AddFile(level, meta));
  }
};

TEST_F(CompactionStreamingTest, StreamMergeMultipleSsts) {
  // Create 3 SST files with interleaved entries to force heap ordering
  uint64_t f1 = 1001, f2 = 1002, f3 = 1003;
  std::string p1 = test_dir_ + "/" + std::to_string(f1) + ".sst";
  std::string p2 = test_dir_ + "/" + std::to_string(f2) + ".sst";
  std::string p3 = test_dir_ + "/" + std::to_string(f3) + ".sst";

  std::vector<std::pair<CedarKey, Descriptor>> entries1;
  std::vector<std::pair<CedarKey, Descriptor>> entries2;
  std::vector<std::pair<CedarKey, Descriptor>> entries3;

  // File 1: entities 1, 4, 7, ...
  for (uint64_t i = 1; i <= 10; i += 3) {
    Descriptor desc = Descriptor::InlineInt(1, static_cast<int64_t>(i * 10));
    CedarKey key(i, EntityType::Vertex, 1, Timestamp(1000 + i), 0, 0, 0, 0);
    entries1.emplace_back(key, desc);
  }

  // File 2: entities 2, 5, 8, ...
  for (uint64_t i = 2; i <= 10; i += 3) {
    Descriptor desc = Descriptor::InlineInt(1, static_cast<int64_t>(i * 10));
    CedarKey key(i, EntityType::Vertex, 1, Timestamp(1000 + i), 0, 0, 0, 0);
    entries2.emplace_back(key, desc);
  }

  // File 3: entities 3, 6, 9, ...
  for (uint64_t i = 3; i <= 10; i += 3) {
    Descriptor desc = Descriptor::InlineInt(1, static_cast<int64_t>(i * 10));
    CedarKey key(i, EntityType::Vertex, 1, Timestamp(1000 + i), 0, 0, 0, 0);
    entries3.emplace_back(key, desc);
  }

  Status s = CreateSstFileWithEntries(p1, entries1, f1, 0);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = CreateSstFileWithEntries(p2, entries2, f2, 0);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = CreateSstFileWithEntries(p3, entries3, f3, 0);
  ASSERT_TRUE(s.ok()) << s.ToString();

  uint64_t sz1 = 0, sz2 = 0, sz3 = 0;
  env_->GetFileSize(p1, &sz1);
  env_->GetFileSize(p2, &sz2);
  env_->GetFileSize(p3, &sz3);

  s = RegisterFileInVersionSet(f1, 0, 1, 10, 1001, 1010, sz1, entries1.size());
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = RegisterFileInVersionSet(f2, 0, 2, 10, 1002, 1010, sz2, entries2.size());
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = RegisterFileInVersionSet(f3, 0, 3, 10, 1003, 1010, sz3, entries3.size());
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto* impl = db_->GetInternalImpl();
  auto* vs = impl->GetVersionSet();

  size_t files_before = vs->GetCurrentVersion()->GetFileCount();
  ASSERT_EQ(files_before, 3);

  // Execute L0 compaction
  s = impl->TEST_DoCompaction(0);
  ASSERT_TRUE(s.ok()) << s.ToString();

  size_t files_after = vs->GetCurrentVersion()->GetFileCount();
  EXPECT_LE(files_after, files_before)
      << "Compaction should not increase file count";

  // Verify old files are deleted
  EXPECT_FALSE(std::filesystem::exists(p1)) << "Old SST file should be removed";
  EXPECT_FALSE(std::filesystem::exists(p2)) << "Old SST file should be removed";
  EXPECT_FALSE(std::filesystem::exists(p3)) << "Old SST file should be removed";

  // Verify new file exists
  auto files_l1 = vs->GetCurrentVersion()->GetFiles(1);
  if (files_l1.empty()) {
    files_l1 = vs->GetCurrentVersion()->GetFiles(0);
  }
  ASSERT_FALSE(files_l1.empty()) << "New SST file should exist after compaction";

  std::string new_path = test_dir_ + "/" + std::to_string(files_l1[0].file_number) + ".sst";
  ASSERT_TRUE(std::filesystem::exists(new_path));

  ZoneColumnarSstReader reader(new_path);
  s = reader.Open();
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(reader.NumEntries(), 10u);

  // Verify entries are in sorted order
  auto* iter = reader.NewIterator();
  uint64_t prev_entity = 0;
  size_t count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    CedarKey key = iter->Key();
    EXPECT_GT(key.entity_id(), prev_entity)
        << "Entries should be sorted by entity_id";
    prev_entity = key.entity_id();
    ++count;
  }
  delete iter;
  EXPECT_EQ(count, 10u);
}

TEST_F(CompactionStreamingTest, StreamMergeWithOverlappingRanges) {
  // Test that entries with overlapping ranges are merged correctly
  uint64_t f1 = 2001, f2 = 2002;
  std::string p1 = test_dir_ + "/" + std::to_string(f1) + ".sst";
  std::string p2 = test_dir_ + "/" + std::to_string(f2) + ".sst";

  std::vector<std::pair<CedarKey, Descriptor>> entries1;
  std::vector<std::pair<CedarKey, Descriptor>> entries2;

  // File 1: entities 1-5 with timestamp 1000
  for (uint64_t i = 1; i <= 5; ++i) {
    Descriptor desc = Descriptor::InlineInt(1, static_cast<int64_t>(i));
    CedarKey key(i, EntityType::Vertex, 1, Timestamp(1000), 0, 0, 0, 0);
    entries1.emplace_back(key, desc);
  }

  // File 2: entities 3-7 with timestamp 2000
  for (uint64_t i = 3; i <= 7; ++i) {
    Descriptor desc = Descriptor::InlineInt(1, static_cast<int64_t>(i * 100));
    CedarKey key(i, EntityType::Vertex, 1, Timestamp(2000), 0, 0, 0, 0);
    entries2.emplace_back(key, desc);
  }

  Status s = CreateSstFileWithEntries(p1, entries1, f1, 0);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = CreateSstFileWithEntries(p2, entries2, f2, 0);
  ASSERT_TRUE(s.ok()) << s.ToString();

  uint64_t sz1 = 0, sz2 = 0;
  env_->GetFileSize(p1, &sz1);
  env_->GetFileSize(p2, &sz2);

  s = RegisterFileInVersionSet(f1, 0, 1, 5, 1000, 1000, sz1, entries1.size());
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = RegisterFileInVersionSet(f2, 0, 3, 7, 2000, 2000, sz2, entries2.size());
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto* impl = db_->GetInternalImpl();
  auto* vs = impl->GetVersionSet();

  s = impl->TEST_DoCompaction(0);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto files_l1 = vs->GetCurrentVersion()->GetFiles(1);
  if (files_l1.empty()) {
    files_l1 = vs->GetCurrentVersion()->GetFiles(0);
  }
  ASSERT_FALSE(files_l1.empty());

  std::string new_path = test_dir_ + "/" + std::to_string(files_l1[0].file_number) + ".sst";
  ZoneColumnarSstReader reader(new_path);
  s = reader.Open();
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(reader.NumEntries(), 10u);  // 5 + 5 = 10 (overlap at 3-5, different timestamps)

  // Verify all expected entries are present in sorted order
  auto* iter = reader.NewIterator();
  size_t count = 0;
  CedarKey prev_key(0, EntityType::Vertex, 0, Timestamp(0), 0, 0, 0, 0);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    CedarKey key = iter->Key();
    if (count > 0) {
      EXPECT_TRUE(prev_key.LessForSorting(key))
          << "Entries should be in sort order";
    }
    prev_key = key;
    ++count;
  }
  delete iter;
  EXPECT_EQ(count, 10u);
}
