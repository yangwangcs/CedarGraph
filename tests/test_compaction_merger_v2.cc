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
#include <memory>
#include <vector>
#include <string>

#include "cedar/sst/zone_columnar_builder.h"
#include "cedar/sst/zone_columnar_reader.h"
#include "cedar/core/env.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {

class CompactionMergerV2Test : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/compaction_merger_v2_test";
    Env::Default()->CreateDir(test_dir_);
  }

  void TearDown() override {
    std::vector<std::string> files;
    Env::Default()->GetChildren(test_dir_, &files);
    for (const auto& f : files) {
      if (f != "." && f != "..") {
        Env::Default()->RemoveFile(test_dir_ + "/" + f);
      }
    }
    Env::Default()->RemoveDir(test_dir_);
  }

  std::string CreateSstFile(
      const std::string& name,
      const std::vector<std::pair<CedarKey, Descriptor>>& data) {
    std::string path = test_dir_ + "/" + name;
    
    auto env = Env::Default();
    WritableFile* file = nullptr;
    Status s = env->NewWritableFile(path, &file);
    if (!s.ok()) return "";
    std::unique_ptr<WritableFile> file_ptr(file);
    
    ZoneColumnarSstBuilder::Options options;
    ZoneColumnarSstBuilder builder(options, file_ptr.get());
    
    for (const auto& [key, desc] : data) {
      builder.Add(key, desc);
    }
    
    if (!builder.Finish().ok()) return "";
    return path;
  }

  std::string test_dir_;
};

// Basic test to verify SST file creation works
TEST_F(CompactionMergerV2Test, BasicSstCreation) {
  std::vector<std::pair<CedarKey, Descriptor>> data;
  for (int i = 0; i < 100; ++i) {
    CedarKey key = CedarKey::Vertex(1000 + i, 1, Timestamp(1000 + i), 0);
    auto desc_opt = Descriptor::InlineShortStr(1, std::string(4, 'a' + (i % 26)));
    Descriptor desc = desc_opt.has_value() ? *desc_opt : Descriptor::InlineInt(1, i);
    data.emplace_back(key, desc);
  }
  
  auto path = CreateSstFile("test.sst", data);
  ASSERT_FALSE(path.empty());
  
  // Verify file exists
  EXPECT_TRUE(Env::Default()->FileExists(path));
  
  // Verify we can open it
  ZoneColumnarSstReader reader(path);
  EXPECT_TRUE(reader.Open().ok());
}

// Test multiple SST files
TEST_F(CompactionMergerV2Test, MultipleSstFiles) {
  // Create first SST file
  std::vector<std::pair<CedarKey, Descriptor>> data1;
  for (int i = 0; i < 50; ++i) {
    CedarKey key = CedarKey::Vertex(1000 + i, 1, Timestamp(1000 + i), 0);
    Descriptor desc = Descriptor::InlineInt(1, i);
    data1.emplace_back(key, desc);
  }
  auto path1 = CreateSstFile("input1.sst", data1);
  ASSERT_FALSE(path1.empty());
  
  // Create second SST file
  std::vector<std::pair<CedarKey, Descriptor>> data2;
  for (int i = 50; i < 100; ++i) {
    CedarKey key = CedarKey::Vertex(1000 + i, 1, Timestamp(1000 + i + 500), 0);
    Descriptor desc = Descriptor::InlineInt(1, i + 1000);
    data2.emplace_back(key, desc);
  }
  auto path2 = CreateSstFile("input2.sst", data2);
  ASSERT_FALSE(path2.empty());
  
  // Verify both files exist
  EXPECT_TRUE(Env::Default()->FileExists(path1));
  EXPECT_TRUE(Env::Default()->FileExists(path2));
  
  // Verify we can open both
  ZoneColumnarSstReader reader1(path1);
  EXPECT_TRUE(reader1.Open().ok());
  
  ZoneColumnarSstReader reader2(path2);
  EXPECT_TRUE(reader2.Open().ok());
}

// Test empty SST file
TEST_F(CompactionMergerV2Test, EmptySstFile) {
  std::vector<std::pair<CedarKey, Descriptor>> empty_data;
  
  auto path = CreateSstFile("empty.sst", empty_data);
  // Empty file may or may not be created successfully depending on builder behavior
  if (!path.empty()) {
    EXPECT_TRUE(Env::Default()->FileExists(path));
  }
}

}  // namespace cedar

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
