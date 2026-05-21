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

#include "cedar/db/manifest.h"
#include "cedar/core/env.h"

using namespace cedar;

TEST(ManifestTest, CompactManifestReturnsNotSupported) {
  std::string test_dir = "/tmp/cedar_manifest_compact_test_" + std::to_string(getpid());
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  ManifestManager manifest(test_dir, Env::Default());
  Status s = manifest.Initialize(true);
  ASSERT_TRUE(s.ok()) << "Failed to initialize manifest: " << s.ToString();

  s = manifest.CompactManifest();
  EXPECT_TRUE(s.IsNotSupportedError()) << "Expected NotSupported, got: " << s.ToString();

  std::filesystem::remove_all(test_dir);
}
