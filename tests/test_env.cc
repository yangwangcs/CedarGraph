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

#include "cedar/core/env.h"
#include "cedar/core/slice.h"

using namespace cedar;

TEST(EnvTest, WriteStringToFileAndReadBack) {
  Env* env = Env::Default();
  std::string fname;
  ASSERT_TRUE(env->GetTestDirectory(&fname).ok());
  fname += "/env_test_file";

  // Write
  std::string content = "Hello, CedarGraph!";
  Status s = WriteStringToFile(env, Slice(content), fname);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Read back
  std::string data;
  s = ReadFileToString(env, fname, &data);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(data, content);

  // Clean up
  env->RemoveFile(fname);
}

TEST(EnvTest, ReadFileToStringMissingFile) {
  Env* env = Env::Default();
  std::string fname;
  ASSERT_TRUE(env->GetTestDirectory(&fname).ok());
  fname += "/env_test_missing";

  std::string data;
  Status s = ReadFileToString(env, fname, &data);
  EXPECT_FALSE(s.ok());
}

TEST(EnvTest, LogFunction) {
  Env* env = Env::Default();
  std::string fname;
  ASSERT_TRUE(env->GetTestDirectory(&fname).ok());
  fname += "/env_test_log";

  Logger* logger = nullptr;
  Status s = env->NewLogger(fname, &logger);
  ASSERT_TRUE(s.ok()) << s.ToString();

  Log(logger, "Test message: %d", 42);

  delete logger;

  // Verify log file contains the message
  std::string data;
  s = ReadFileToString(env, fname, &data);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_NE(data.find("Test message: 42"), std::string::npos);

  env->RemoveFile(fname);
}

TEST(EnvTest, LogNullLogger) {
  // Should not crash
  Log(nullptr, "This should be silently ignored");
}
