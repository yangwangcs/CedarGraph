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

#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"

using namespace cedar;

class LsmEngineLifecycleTest : public ::testing::Test {
 protected:
  std::string GetTestDir() {
    return "/tmp/cedar_lsm_lifecycle_" + std::to_string(getpid()) + "_" +
           std::to_string(reinterpret_cast<uintptr_t>(this));
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }

  std::string test_dir_;
};

TEST_F(LsmEngineLifecycleTest, OpenAndCloseIsSafe) {
  test_dir_ = GetTestDir();
  std::filesystem::remove_all(test_dir_);
  std::filesystem::create_directories(test_dir_);

  {
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = false;
    options.enable_accumulated_flush = false;

    auto engine = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
    Status s = engine->Open();
    EXPECT_TRUE(s.ok()) << "Open failed: " << s.ToString();
    // Destructor calls Close() implicitly
  }

  // Verify directory still exists (no crash / throw)
  EXPECT_TRUE(std::filesystem::exists(test_dir_));
}

TEST_F(LsmEngineLifecycleTest, DoubleCloseIsSafe) {
  test_dir_ = GetTestDir();
  std::filesystem::remove_all(test_dir_);
  std::filesystem::create_directories(test_dir_);

  CedarOptions options;
  options.create_if_missing = true;
  options.enable_skeleton_cache = false;
  options.enable_accumulated_flush = false;

  auto engine = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
  Status s = engine->Open();
  EXPECT_TRUE(s.ok()) << "Open failed: " << s.ToString();

  s = engine->Close();
  EXPECT_TRUE(s.ok()) << "First Close failed: " << s.ToString();

  s = engine->Close();
  EXPECT_TRUE(s.ok()) << "Second Close failed: " << s.ToString();
}
