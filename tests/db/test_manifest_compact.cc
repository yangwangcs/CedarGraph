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

#include "cedar/db/manifest.h"
#include "cedar/core/env.h"

using namespace cedar;

TEST(ManifestTest, CompactManifestReturnsNotSupportedForNullVersionSet) {
  std::string test_dir = "/tmp/cedar_manifest_compact_test_" + std::to_string(getpid());
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  ManifestManager manifest(test_dir, Env::Default());
  Status s = manifest.Initialize(true);
  ASSERT_TRUE(s.ok()) << "Failed to initialize manifest: " << s.ToString();

  s = manifest.CompactManifest(nullptr);
  EXPECT_TRUE(s.IsInvalidArgument()) << "Expected InvalidArgument, got: " << s.ToString();

  std::filesystem::remove_all(test_dir);
}

TEST(ManifestTest, CompactManifestRewritesManifestAtomically) {
  std::string test_dir = "/tmp/cedar_manifest_compact_test_" + std::to_string(getpid());
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  Env* env = Env::Default();
  ManifestManager manifest(test_dir, env);
  Status s = manifest.Initialize(true);
  ASSERT_TRUE(s.ok()) << "Failed to initialize manifest: " << s.ToString();

  // Build a VersionSet with some files
  VersionSet version_set;
  version_set.SetLastSequence(42);
  version_set.SetLogNumber(7);

  FileMetaData meta1;
  meta1.file_number = 1001;
  meta1.level = 0;
  meta1.file_size = 4096;
  meta1.smallest_entity_id = 1;
  meta1.largest_entity_id = 100;
  meta1.smallest_timestamp = 1000;
  meta1.largest_timestamp = 2000;
  meta1.num_entries = 100;

  FileMetaData meta2;
  meta2.file_number = 1002;
  meta2.level = 1;
  meta2.file_size = 8192;
  meta2.smallest_entity_id = 50;
  meta2.largest_entity_id = 150;
  meta2.smallest_timestamp = 1500;
  meta2.largest_timestamp = 3000;
  meta2.num_entries = 100;

  std::shared_ptr<Version> v;
  s = version_set.ApplyEdit(ManifestEdit::AddFile(0, meta1), &v);
  ASSERT_TRUE(s.ok());
  s = version_set.ApplyEdit(ManifestEdit::AddFile(1, meta2), &v);
  ASSERT_TRUE(s.ok());

  // Also log these edits to the original manifest so we have history
  s = manifest.LogEdit(ManifestEdit::AddFile(0, meta1));
  ASSERT_TRUE(s.ok());
  s = manifest.LogEdit(ManifestEdit::AddFile(1, meta2));
  ASSERT_TRUE(s.ok());

  // Compact the manifest
  s = manifest.CompactManifest(&version_set);
  ASSERT_TRUE(s.ok()) << "CompactManifest failed: " << s.ToString();

  // Verify a new MANIFEST file was created
  std::string current_manifest;
  {
    std::ifstream cf(test_dir + "/CURRENT");
    std::getline(cf, current_manifest);
    cf.close();
  }
  EXPECT_EQ(current_manifest, "MANIFEST-2");

  // Verify the new manifest exists
  EXPECT_TRUE(std::filesystem::exists(test_dir + "/MANIFEST-2"));

  // Verify we can reload the version from the new manifest
  ManifestManager manifest2(test_dir, env);
  s = manifest2.Initialize(false);
  ASSERT_TRUE(s.ok()) << "Failed to init manifest2: " << s.ToString();

  std::shared_ptr<Version> loaded_version;
  uint64_t next_file_number = 0;
  uint64_t last_sequence = 0;
  s = manifest2.LoadCurrentVersion(&loaded_version, &next_file_number, &last_sequence);
  ASSERT_TRUE(s.ok()) << "LoadCurrentVersion failed: " << s.ToString();

  EXPECT_EQ(loaded_version->GetFileCount(), 2u);
  EXPECT_EQ(last_sequence, 42u);

  std::filesystem::remove_all(test_dir);
}
