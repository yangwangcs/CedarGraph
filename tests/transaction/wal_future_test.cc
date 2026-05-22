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
#include <chrono>
#include <filesystem>

#include "cedar/transaction/wal.h"

using namespace cedar;

TEST(WalFutureTest, AsyncWriteReturnsOkViaFuture) {
  std::string wal_dir = "/tmp/test_wal_future_" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  std::filesystem::remove_all(wal_dir);
  std::filesystem::create_directories(wal_dir);

  WalOptions options;
  options.group_commit_timeout_us = 1000;  // 1ms group commit

  WalWriter writer(wal_dir, cedar::Env::Default(), options);
  ASSERT_TRUE(writer.Open().ok());

  WalBatch batch;
  CedarKey key = CedarKey::Vertex(1, VertexColumnId(0), Timestamp(100), 0, 0);
  batch.Put(key, Descriptor::InlineInt(0, 42), Timestamp(100));

  WalWriter::AsyncResult async;
  ASSERT_TRUE(writer.WriteBatchAsync(batch, &async).ok());
  EXPECT_GT(async.sequence, 0U);

  Status result = async.future.get();
  EXPECT_TRUE(result.ok()) << result.ToString();

  ASSERT_TRUE(writer.Close().ok());
  std::filesystem::remove_all(wal_dir);
}

TEST(WalFutureTest, NullAsyncResultReturnsError) {
  std::string wal_dir = "/tmp/test_wal_future_null_" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  std::filesystem::remove_all(wal_dir);
  std::filesystem::create_directories(wal_dir);

  WalOptions options;
  options.group_commit_timeout_us = 1000;

  WalWriter writer(wal_dir, cedar::Env::Default(), options);
  ASSERT_TRUE(writer.Open().ok());

  WalBatch batch;
  CedarKey key = CedarKey::Vertex(1, VertexColumnId(0), Timestamp(100), 0, 0);
  batch.Put(key, Descriptor::InlineInt(0, 42), Timestamp(100));

  Status s = writer.WriteBatchAsync(batch, nullptr);
  EXPECT_FALSE(s.ok());

  ASSERT_TRUE(writer.Close().ok());
  std::filesystem::remove_all(wal_dir);
}
