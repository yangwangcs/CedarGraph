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
#include <fstream>

#include "cedar/db/graph_db.h"
#include "cedar/db/manifest.h"
#include "cedar/core/env.h"
#include "cedar/sst/zone_columnar_reader.h"
#include "cedar/sst/sst_builder_factory.h"

using namespace cedar;

class RepairDBTest : public ::testing::Test {
 protected:
  std::string test_dir_;
  Env* env_ = nullptr;

  void SetUp() override {
    test_dir_ = "/tmp/cedar_repair_test_" + std::to_string(getpid());
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
    env_ = Env::Default();
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }

  Status CreateSstFile(uint64_t file_number,
                       uint64_t start_entity,
                       uint64_t end_entity) {
    std::string path = test_dir_ + "/" + std::to_string(file_number) + ".sst";
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
};

TEST_F(RepairDBTest, RepairDBRebuildsManifestFromSSTs) {
  // Create two SST files directly (simulating a manifest-loss scenario)
  Status s = CreateSstFile(3001, 1, 50);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = CreateSstFile(3002, 50, 100);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Verify no CURRENT / manifest exists yet
  EXPECT_FALSE(std::filesystem::exists(test_dir_ + "/CURRENT"));

  // Run repair
  CedarGraphOptions options;
  options.create_if_missing = false;
  s = CedarGraphDB::RepairDB(test_dir_, options);
  ASSERT_TRUE(s.ok()) << "RepairDB failed: " << s.ToString();

  // Verify CURRENT and MANIFEST now exist
  EXPECT_TRUE(std::filesystem::exists(test_dir_ + "/CURRENT"));
  EXPECT_TRUE(std::filesystem::exists(test_dir_ + "/MANIFEST-1"));

  // Verify the manifest loads and contains the 2 files
  ManifestManager manifest(test_dir_, env_);
  s = manifest.Initialize(false);
  ASSERT_TRUE(s.ok()) << "Manifest init failed: " << s.ToString();

  std::shared_ptr<Version> version;
  uint64_t next_file = 0;
  uint64_t last_seq = 0;
  s = manifest.LoadCurrentVersion(&version, &next_file, &last_seq);
  ASSERT_TRUE(s.ok()) << "LoadCurrentVersion failed: " << s.ToString();

  EXPECT_EQ(version->GetFileCount(), 2u);
}

TEST_F(RepairDBTest, RepairDBSkipsCorruptSSTs) {
  // Create one valid SST
  Status s = CreateSstFile(4001, 1, 20);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Create a corrupt SST (empty file)
  {
    std::string corrupt_path = test_dir_ + "/4002.sst";
    std::ofstream ofs(corrupt_path);
    ofs << "not-a-valid-sst";
    ofs.close();
  }

  s = CedarGraphDB::RepairDB(test_dir_, CedarGraphOptions());
  ASSERT_TRUE(s.ok()) << "RepairDB failed: " << s.ToString();

  ManifestManager manifest(test_dir_, env_);
  s = manifest.Initialize(false);
  ASSERT_TRUE(s.ok());

  std::shared_ptr<Version> version;
  uint64_t nf = 0, ls = 0;
  s = manifest.LoadCurrentVersion(&version, &nf, &ls);
  ASSERT_TRUE(s.ok());

  // Only the valid SST should be in the manifest
  EXPECT_EQ(version->GetFileCount(), 1u);
}

TEST_F(RepairDBTest, RepairDBReturnsErrorForMissingPath) {
  Status s = CedarGraphDB::RepairDB("/nonexistent/path/that/does/not/exist",
                                     CedarGraphOptions());
  EXPECT_TRUE(s.IsIOError()) << "Expected IOError for missing path, got: " << s.ToString();
}
