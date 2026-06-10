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
#include <cstdio>

#include "cedar/db/graph_db.h"
#include "cedar/db/graph_db_impl.h"
#include "cedar/sst/zone_columnar_reader.h"
#include "cedar/sst/sst_builder_factory.h"
#include "cedar/core/env.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class CompactionTest : public ::testing::Test {
 protected:
  std::string test_dir_;
  CedarGraphDB* db_ = nullptr;
  Env* env_ = nullptr;

  void SetUp() override {
    test_dir_ = "/tmp/cedar_compaction_test_" + std::to_string(getpid());
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

  // 在指定路径创建一个简单的 SST 文件，包含 [start, end) 的 entity
  Status CreateSstFile(const std::string& path,
                       uint64_t start_entity,
                       uint64_t end_entity,
                       uint64_t file_number,
                       int level) {
    WritableFile* file = nullptr;
    Status s = env_->NewWritableFile(path, &file);
    if (!s.ok()) return s;

    auto builder = SstBuilderFactory::Create(file, test_dir_);
    for (uint64_t e = start_entity; e < end_entity; ++e) {
      Descriptor desc = Descriptor::InlineInt(1, static_cast<int64_t>(e));
      CedarKey key(e, EntityType::Vertex, 1, Timestamp(1000 + e),
                   static_cast<uint16_t>(file_number), 0, 0, 0);
      builder->Add(key, desc, Timestamp(0));
    }

    s = builder->Finish();
    delete file;
    if (!s.ok()) {
      env_->RemoveFile(path);
      return s;
    }
    return Status::OK();
  }

  // 将文件注册到 VersionSet
  Status RegisterFileInVersionSet(uint64_t file_number,
                                  int level,
                                  uint64_t start_entity,
                                  uint64_t end_entity,
                                  uint64_t file_size) {
    auto* impl = db_->GetInternalImpl();
    auto* vs = impl->GetVersionSet();

    FileMetaData meta;
    meta.file_number = file_number;
    meta.level = level;
    meta.file_size = file_size;
    meta.smallest_entity_id = start_entity;
    meta.largest_entity_id = end_entity - 1;
    meta.smallest_timestamp = 1000 + start_entity;
    meta.largest_timestamp = 1000 + end_entity - 1;
    meta.num_entries = end_entity - start_entity;

    return vs->ApplyEdit(ManifestEdit::AddFile(level, meta));
  }
};

TEST_F(CompactionTest, DoCompactionMergesLevel0Files) {
  // 创建两个 L0 SST 文件，entity 范围有重叠
  uint64_t f1 = 1001;
  uint64_t f2 = 1002;
  std::string p1 = test_dir_ + "/" + std::to_string(f1) + ".sst";
  std::string p2 = test_dir_ + "/" + std::to_string(f2) + ".sst";

  Status s = CreateSstFile(p1, 1, 50, f1, 0);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = CreateSstFile(p2, 30, 80, f2, 0);
  ASSERT_TRUE(s.ok()) << s.ToString();

  uint64_t sz1 = 0, sz2 = 0;
  env_->GetFileSize(p1, &sz1);
  env_->GetFileSize(p2, &sz2);

  s = RegisterFileInVersionSet(f1, 0, 1, 50, sz1);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = RegisterFileInVersionSet(f2, 0, 30, 80, sz2);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto* impl = db_->GetInternalImpl();
  auto* vs = impl->GetVersionSet();

  size_t files_before = vs->GetCurrentVersion()->GetFileCount();
  ASSERT_EQ(files_before, 2);

  // 执行 L0 压缩
  s = impl->TEST_DoCompaction(0);
  ASSERT_TRUE(s.ok()) << s.ToString();

  size_t files_after = vs->GetCurrentVersion()->GetFileCount();

  // 压缩后文件数应减少或保持不变（合并两个文件为一个）
  EXPECT_LE(files_after, files_before)
      << "Compaction should not increase file count";

  // 验证旧文件已被删除
  EXPECT_FALSE(std::filesystem::exists(p1)) << "Old SST file should be removed";
  EXPECT_FALSE(std::filesystem::exists(p2)) << "Old SST file should be removed";

  // 验证新文件存在
  auto files_l1 = vs->GetCurrentVersion()->GetFiles(1);
  if (files_l1.empty()) {
    files_l1 = vs->GetCurrentVersion()->GetFiles(0);
  }
  ASSERT_FALSE(files_l1.empty()) << "New SST file should exist after compaction";

  // 验证新文件可以被读取且包含所有条目（含重叠范围的重复条目）
  std::string new_path = test_dir_ + "/" + std::to_string(files_l1[0].file_number) + ".sst";
  ASSERT_TRUE(std::filesystem::exists(new_path));

  ZoneColumnarSstReader reader(new_path);
  s = reader.Open();
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(reader.NumEntries(), 99u);  // 49 + 50 = 99 (含重叠 30-49)
}

TEST_F(CompactionTest, DoCompactionOnEmptyVersionSet) {
  auto* impl = db_->GetInternalImpl();
  Status s = impl->TEST_DoCompaction(0);
  EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST_F(CompactionTest, CompactRangeMergesOverlappingFiles) {
  // Create two L0 SST files with overlapping entity ranges
  uint64_t f1 = 2001;
  uint64_t f2 = 2002;
  std::string p1 = test_dir_ + "/" + std::to_string(f1) + ".sst";
  std::string p2 = test_dir_ + "/" + std::to_string(f2) + ".sst";

  Status s = CreateSstFile(p1, 1, 50, f1, 0);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = CreateSstFile(p2, 30, 80, f2, 0);
  ASSERT_TRUE(s.ok()) << s.ToString();

  uint64_t sz1 = 0, sz2 = 0;
  env_->GetFileSize(p1, &sz1);
  env_->GetFileSize(p2, &sz2);

  s = RegisterFileInVersionSet(f1, 0, 1, 50, sz1);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = RegisterFileInVersionSet(f2, 0, 30, 80, sz2);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Compact only entity range [20, 60]
  CompactRangeOptions cro;
  cro.start_entity_id = 20;
  cro.end_entity_id = 60;
  s = db_->CompactRange(cro);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto* impl = db_->GetInternalImpl();
  auto* vs = impl->GetVersionSet();

  // After range compaction, the total file count should be <= 2
  // (either both original files are gone + one new file created,
  //  or one original remains if it was fully outside the range)
  size_t file_count = vs->GetCurrentVersion()->GetFileCount();
  EXPECT_LE(file_count, 2u);

  // The old overlapping files should have been removed
  EXPECT_FALSE(std::filesystem::exists(p1))
      << "Old SST f1 should be removed after range compaction";
  EXPECT_FALSE(std::filesystem::exists(p2))
      << "Old SST f2 should be removed after range compaction";
}

TEST_F(CompactionTest, CompactRangeOnEmptyRangeIsNoOp) {
  CompactRangeOptions cro;
  cro.start_entity_id = 1000;
  cro.end_entity_id = 2000;
  Status s = db_->CompactRange(cro);
  EXPECT_TRUE(s.ok()) << s.ToString();
}
