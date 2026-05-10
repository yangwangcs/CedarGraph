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
#include <string>
#include <vector>

#include "src/raft/partition_log_store.h"
#include "raft_service.pb.h"

namespace cedar {
namespace raft {

namespace fs = std::filesystem;

using LogEntry = cedar::raft::internal::LogEntry;

class LogCompactionTest : public ::testing::Test {
 protected:
  std::string test_dir_ = "/tmp/test_log_compaction";

  void SetUp() override {
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);
  }

  void TearDown() override {
    fs::remove_all(test_dir_);
  }

  LogEntry MakeEntry(uint64_t index, uint64_t term, const std::string& cmd) {
    LogEntry entry;
    entry.set_index(index);
    entry.set_term(term);
    entry.set_command(cmd);
    return entry;
  }
};

TEST_F(LogCompactionTest, BasicAppendAndRetrieve) {
  PartitionLogStore store(1, test_dir_);
  ASSERT_TRUE(store.Initialize().ok());

  std::vector<LogEntry> entries;
  for (uint64_t i = 1; i <= 10; ++i) {
    entries.push_back(MakeEntry(i, 1, "cmd" + std::to_string(i)));
  }
  ASSERT_TRUE(store.AppendEntries(entries).ok());

  EXPECT_EQ(store.GetFirstLogIndex(), 1);
  EXPECT_EQ(store.GetLastLogIndex(), 10);

  auto result = store.GetEntry(5);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().index(), 5);
  EXPECT_EQ(result.ValueOrDie().command(), "cmd5");
}

TEST_F(LogCompactionTest, CompactCommitsAndTruncates) {
  PartitionLogStore store(1, test_dir_);
  ASSERT_TRUE(store.Initialize().ok());

  std::vector<LogEntry> entries;
  for (uint64_t i = 1; i <= 100; ++i) {
    entries.push_back(MakeEntry(i, 1, "cmd" + std::to_string(i)));
  }
  ASSERT_TRUE(store.AppendEntries(entries).ok());

  // Commit up to index 50
  store.SetCommittedIndex(50);

  // Compact up to 40
  ASSERT_TRUE(store.Compact(40).ok());

  EXPECT_EQ(store.GetFirstLogIndex(), 41);
  EXPECT_EQ(store.GetLastLogIndex(), 100);

  // Compacted entries should not be found
  auto missing = store.GetEntry(10);
  EXPECT_FALSE(missing.ok());

  // Non-compacted entries should still be found
  auto present = store.GetEntry(50);
  ASSERT_TRUE(present.ok());
  EXPECT_EQ(present.ValueOrDie().command(), "cmd50");

  // Range query should work
  auto range = store.GetEntries(41, 45);
  EXPECT_EQ(range.size(), 5);
}

TEST_F(LogCompactionTest, CompactBeyondCommittedFails) {
  PartitionLogStore store(1, test_dir_);
  ASSERT_TRUE(store.Initialize().ok());

  std::vector<LogEntry> entries;
  for (uint64_t i = 1; i <= 10; ++i) {
    entries.push_back(MakeEntry(i, 1, "cmd" + std::to_string(i)));
  }
  ASSERT_TRUE(store.AppendEntries(entries).ok());

  store.SetCommittedIndex(5);

  // Compact beyond committed index should fail
  auto status = store.Compact(8);
  EXPECT_FALSE(status.ok());
}

TEST_F(LogCompactionTest, SnapshotCreateAndLoad) {
  PartitionLogStore store(1, test_dir_);
  ASSERT_TRUE(store.Initialize().ok());

  std::string state = "state_machine_data_v1";
  ASSERT_TRUE(store.CreateSnapshot(100, 5, state).ok());
  EXPECT_TRUE(store.HasSnapshot());

  uint64_t index = 0, term = 0;
  std::string loaded_state;
  ASSERT_TRUE(store.GetSnapshotData(&index, &term, &loaded_state).ok());
  EXPECT_EQ(index, 100);
  EXPECT_EQ(term, 5);
  EXPECT_EQ(loaded_state, state);
}

TEST_F(LogCompactionTest, CompactRewritesLogFile) {
  PartitionLogStore store(1, test_dir_);
  ASSERT_TRUE(store.Initialize().ok());

  std::vector<LogEntry> entries;
  for (uint64_t i = 1; i <= 20; ++i) {
    entries.push_back(MakeEntry(i, 1, "cmd" + std::to_string(i)));
  }
  ASSERT_TRUE(store.AppendEntries(entries).ok());

  store.SetCommittedIndex(15);
  ASSERT_TRUE(store.Compact(10).ok());

  // Close and reopen to verify disk state
  ASSERT_TRUE(store.Close().ok());

  PartitionLogStore store2(1, test_dir_);
  ASSERT_TRUE(store2.Initialize().ok());

  EXPECT_EQ(store2.GetFirstLogIndex(), 11);
  EXPECT_EQ(store2.GetLastLogIndex(), 20);

  auto e = store2.GetEntry(15);
  ASSERT_TRUE(e.ok());
  EXPECT_EQ(e.ValueOrDie().command(), "cmd15");
}

TEST_F(LogCompactionTest, SnapshotSurvivesReopen) {
  PartitionLogStore store(1, test_dir_);
  ASSERT_TRUE(store.Initialize().ok());

  ASSERT_TRUE(store.CreateSnapshot(50, 3, "snapshot_v1").ok());
  ASSERT_TRUE(store.Close().ok());

  PartitionLogStore store2(1, test_dir_);
  ASSERT_TRUE(store2.Initialize().ok());
  EXPECT_TRUE(store2.HasSnapshot());

  uint64_t idx = 0, term = 0;
  std::string data;
  ASSERT_TRUE(store2.GetSnapshotData(&idx, &term, &data).ok());
  EXPECT_EQ(idx, 50);
  EXPECT_EQ(term, 3);
  EXPECT_EQ(data, "snapshot_v1");
}

TEST_F(LogCompactionTest, GetEntriesClampsToFirstIndex) {
  PartitionLogStore store(1, test_dir_);
  ASSERT_TRUE(store.Initialize().ok());

  std::vector<LogEntry> entries;
  for (uint64_t i = 1; i <= 10; ++i) {
    entries.push_back(MakeEntry(i, 1, "cmd" + std::to_string(i)));
  }
  ASSERT_TRUE(store.AppendEntries(entries).ok());
  store.SetCommittedIndex(10);
  ASSERT_TRUE(store.Compact(5).ok());

  // Request starting before first_index should be clamped
  auto range = store.GetEntries(1, 10);
  EXPECT_EQ(range.size(), 5);  // 6..10
  EXPECT_EQ(range.front().index(), 6);
}

}  // namespace raft
}  // namespace cedar
