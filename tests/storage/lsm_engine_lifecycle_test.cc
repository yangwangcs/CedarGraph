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
#include <chrono>
#include <thread>

#define private public
#include "cedar/storage/lsm_engine.h"
#undef private
#include "cedar/storage/cedar_options.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

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

TEST_F(LsmEngineLifecycleTest, ReopenAllowsWritesAndFlushes) {
  test_dir_ = GetTestDir();
  std::filesystem::remove_all(test_dir_);
  std::filesystem::create_directories(test_dir_);

  CedarOptions options;
  options.create_if_missing = true;
  options.enable_skeleton_cache = false;
  options.enable_accumulated_flush = false;

  auto engine = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
  Status s = engine->Open();
  ASSERT_TRUE(s.ok()) << "Open failed: " << s.ToString();

  s = engine->Close();
  ASSERT_TRUE(s.ok()) << "Close failed: " << s.ToString();

  s = engine->Open();
  ASSERT_TRUE(s.ok()) << "Reopen failed: " << s.ToString();

  CedarKey key = CedarKey::Vertex(1001, 1, Timestamp(100));
  s = engine->Put(key, Descriptor::InlineInt(1, 42), Timestamp(2));
  ASSERT_TRUE(s.ok()) << "Put after reopen failed: " << s.ToString();

  s = engine->ForceFlush();
  EXPECT_TRUE(s.ok()) << "ForceFlush after reopen failed: " << s.ToString();
}

TEST_F(LsmEngineLifecycleTest, CloseWakesTTLCleanupThreadPromptly) {
  test_dir_ = GetTestDir();
  std::filesystem::remove_all(test_dir_);
  std::filesystem::create_directories(test_dir_);

  CedarOptions options;
  options.create_if_missing = true;
  options.enable_skeleton_cache = false;
  options.enable_accumulated_flush = false;

  auto engine = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
  Status s = engine->Open();
  ASSERT_TRUE(s.ok()) << "Open failed: " << s.ToString();
  engine->SetTTLConfig(1, true, test_dir_ + "/archive");
  engine->StartTTLCleanupThread(30);

  auto start = std::chrono::steady_clock::now();
  s = engine->Close();
  auto elapsed = std::chrono::steady_clock::now() - start;

  ASSERT_TRUE(s.ok()) << "Close failed: " << s.ToString();
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            500);
}

TEST_F(LsmEngineLifecycleTest, CloseWakesDisabledAutoCompactionThreadPromptly) {
  test_dir_ = GetTestDir();
  std::filesystem::remove_all(test_dir_);
  std::filesystem::create_directories(test_dir_);

  CedarOptions options;
  options.create_if_missing = true;
  options.enable_skeleton_cache = false;
  options.enable_accumulated_flush = false;
  options.size_tiered_config.enable_background_compaction = false;

  auto engine = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
  Status s = engine->Open();
  ASSERT_TRUE(s.ok()) << "Open failed: " << s.ToString();

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  auto start = std::chrono::steady_clock::now();
  s = engine->Close();
  auto elapsed = std::chrono::steady_clock::now() - start;

  ASSERT_TRUE(s.ok()) << "Close failed: " << s.ToString();
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            500);
}
