#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include "cedar/gcn/checkpoint_store.h"

namespace {

namespace fs = std::filesystem;

class CheckpointStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    directory_ = fs::temp_directory_path() /
                 ("cedar_gcn_checkpoint_test_" +
                  std::to_string(reinterpret_cast<uintptr_t>(this)));
    fs::remove_all(directory_);
    fs::create_directories(directory_);
  }

  void TearDown() override { fs::remove_all(directory_); }

  fs::path CheckpointPath(uint32_t partition_id) const {
    char name[64];
    std::snprintf(name, sizeof(name), "partition_%010u.chkpt", partition_id);
    return directory_ / name;
  }

  fs::path TempPath(uint32_t partition_id) const {
    char name[64];
    std::snprintf(name, sizeof(name), "partition_%010u.chkpt.tmp", partition_id);
    return directory_ / name;
  }

  std::vector<char> ReadFile(const fs::path& path) const {
    std::ifstream input(path, std::ios::binary);
    return std::vector<char>(std::istreambuf_iterator<char>(input),
                             std::istreambuf_iterator<char>());
  }

  void WriteFile(const fs::path& path, const std::vector<char>& bytes) const {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }

  fs::path directory_;
};

TEST_F(CheckpointStoreTest, SaveLoadRoundTripPersistsAllFields) {
  cedar::gcn::CheckpointStore store(directory_.string());
  cedar::gcn::PartitionCheckpoint checkpoint;
  checkpoint.partition_id = 7;
  checkpoint.partition_epoch = 11;
  checkpoint.applied_offset = 12345;
  checkpoint.applied_version = 67890;
  checkpoint.tmv_snapshot_id = "snapshot-alpha";

  ASSERT_TRUE(store.Save(checkpoint).ok());

  auto loaded = store.Load(7);
  ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
  ASSERT_TRUE(loaded.ValueOrDie().has_value());
  EXPECT_EQ(loaded.ValueOrDie()->partition_id, checkpoint.partition_id);
  EXPECT_EQ(loaded.ValueOrDie()->partition_epoch, checkpoint.partition_epoch);
  EXPECT_EQ(loaded.ValueOrDie()->applied_offset, checkpoint.applied_offset);
  EXPECT_EQ(loaded.ValueOrDie()->applied_version, checkpoint.applied_version);
  EXPECT_EQ(loaded.ValueOrDie()->tmv_snapshot_id, checkpoint.tmv_snapshot_id);
}

TEST_F(CheckpointStoreTest, MissingCheckpointReturnsEmptyOptional) {
  cedar::gcn::CheckpointStore store(directory_.string());

  auto loaded = store.Load(42);

  ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
  EXPECT_FALSE(loaded.ValueOrDie().has_value());
}

TEST_F(CheckpointStoreTest, ChecksumCorruptionIsRejected) {
  cedar::gcn::CheckpointStore store(directory_.string());
  cedar::gcn::PartitionCheckpoint checkpoint;
  checkpoint.partition_id = 3;
  checkpoint.partition_epoch = 1;
  checkpoint.applied_offset = 2;
  checkpoint.applied_version = 3;
  checkpoint.tmv_snapshot_id = "snapshot";
  ASSERT_TRUE(store.Save(checkpoint).ok());

  auto bytes = ReadFile(CheckpointPath(3));
  ASSERT_GT(bytes.size(), 16u);
  bytes.back() ^= 0x40;
  WriteFile(CheckpointPath(3), bytes);

  auto loaded = store.Load(3);

  ASSERT_FALSE(loaded.ok());
  EXPECT_TRUE(loaded.status().IsCorruption()) << loaded.status().ToString();
}

TEST_F(CheckpointStoreTest, TornTempWriteDoesNotReplaceLastGoodCheckpoint) {
  cedar::gcn::CheckpointStore store(directory_.string());
  cedar::gcn::PartitionCheckpoint checkpoint;
  checkpoint.partition_id = 9;
  checkpoint.partition_epoch = 4;
  checkpoint.applied_offset = 100;
  checkpoint.applied_version = 200;
  checkpoint.tmv_snapshot_id = "stable";
  ASSERT_TRUE(store.Save(checkpoint).ok());

  WriteFile(TempPath(9), std::vector<char>{'t', 'o', 'r', 'n'});

  auto loaded = store.Load(9);

  ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
  ASSERT_TRUE(loaded.ValueOrDie().has_value());
  EXPECT_EQ(loaded.ValueOrDie()->applied_offset, 100u);
  EXPECT_EQ(loaded.ValueOrDie()->tmv_snapshot_id, "stable");
}

TEST_F(CheckpointStoreTest, RemoveDeletesOnlySelectedPartition) {
  cedar::gcn::CheckpointStore store(directory_.string());
  cedar::gcn::PartitionCheckpoint first;
  first.partition_id = 1;
  first.partition_epoch = 10;
  first.applied_offset = 20;
  first.applied_version = 30;
  first.tmv_snapshot_id = "one";
  cedar::gcn::PartitionCheckpoint second = first;
  second.partition_id = 2;
  second.tmv_snapshot_id = "two";
  ASSERT_TRUE(store.Save(first).ok());
  ASSERT_TRUE(store.Save(second).ok());

  ASSERT_TRUE(store.Remove(1).ok());

  auto removed = store.Load(1);
  ASSERT_TRUE(removed.ok()) << removed.status().ToString();
  EXPECT_FALSE(removed.ValueOrDie().has_value());

  auto kept = store.Load(2);
  ASSERT_TRUE(kept.ok()) << kept.status().ToString();
  ASSERT_TRUE(kept.ValueOrDie().has_value());
  EXPECT_EQ(kept.ValueOrDie()->tmv_snapshot_id, "two");
}

TEST_F(CheckpointStoreTest, GeneratedFilenamesAreSafeForAllPartitionIds) {
  cedar::gcn::CheckpointStore store(directory_.string());
  cedar::gcn::PartitionCheckpoint checkpoint;
  checkpoint.partition_id = UINT32_MAX;
  checkpoint.partition_epoch = 1;
  checkpoint.applied_offset = 2;
  checkpoint.applied_version = 3;
  checkpoint.tmv_snapshot_id = "../not-a-path";

  ASSERT_TRUE(store.Save(checkpoint).ok());

  EXPECT_TRUE(fs::exists(CheckpointPath(UINT32_MAX)));
  EXPECT_FALSE(fs::exists(directory_.parent_path() / "not-a-path"));
  auto loaded = store.Load(UINT32_MAX);
  ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
  ASSERT_TRUE(loaded.ValueOrDie().has_value());
  EXPECT_EQ(loaded.ValueOrDie()->partition_id, UINT32_MAX);
  EXPECT_EQ(loaded.ValueOrDie()->tmv_snapshot_id, "../not-a-path");
}

TEST_F(CheckpointStoreTest, RejectsSymlinkedDirectoryAndTempPath) {
  const fs::path real_dir = directory_ / "real";
  const fs::path link_dir = directory_ / "link";
  fs::create_directories(real_dir);
  fs::create_directory_symlink(real_dir, link_dir);

  cedar::gcn::CheckpointStore symlinked_store(link_dir.string());
  cedar::gcn::PartitionCheckpoint checkpoint;
  checkpoint.partition_id = 6;
  checkpoint.partition_epoch = 1;
  checkpoint.applied_offset = 2;
  checkpoint.applied_version = 3;
  checkpoint.tmv_snapshot_id = "snapshot";
  EXPECT_FALSE(symlinked_store.Save(checkpoint).ok());

  cedar::gcn::CheckpointStore store(directory_.string());
  const fs::path outside = directory_.parent_path() / "outside_checkpoint_target";
  fs::remove(outside);
  fs::create_symlink(outside, TempPath(6));
  EXPECT_FALSE(store.Save(checkpoint).ok());
  EXPECT_FALSE(fs::exists(outside));
}

TEST_F(CheckpointStoreTest, MalformedUnknownTruncatedAndMismatchedFilesAreRejected) {
  cedar::gcn::CheckpointStore store(directory_.string());

  WriteFile(CheckpointPath(5), std::vector<char>{'b', 'a', 'd'});
  auto malformed = store.Load(5);
  ASSERT_FALSE(malformed.ok());
  EXPECT_TRUE(malformed.status().IsCorruption()) << malformed.status().ToString();

  cedar::gcn::PartitionCheckpoint checkpoint;
  checkpoint.partition_id = 5;
  checkpoint.partition_epoch = 1;
  checkpoint.applied_offset = 2;
  checkpoint.applied_version = 3;
  checkpoint.tmv_snapshot_id = "snapshot";
  ASSERT_TRUE(store.Save(checkpoint).ok());

  auto bytes = ReadFile(CheckpointPath(5));
  ASSERT_GT(bytes.size(), 12u);

  auto unknown_version = bytes;
  unknown_version[8] = 99;
  WriteFile(CheckpointPath(5), unknown_version);
  auto unknown = store.Load(5);
  ASSERT_FALSE(unknown.ok());
  EXPECT_TRUE(unknown.status().IsCorruption()) << unknown.status().ToString();

  WriteFile(CheckpointPath(5), std::vector<char>(bytes.begin(), bytes.begin() + 12));
  auto truncated = store.Load(5);
  ASSERT_FALSE(truncated.ok());
  EXPECT_TRUE(truncated.status().IsCorruption()) << truncated.status().ToString();

  ASSERT_TRUE(store.Save(checkpoint).ok());
  auto mismatch = ReadFile(CheckpointPath(5));
  ASSERT_GT(mismatch.size(), 20u);
  mismatch[16] = 6;
  WriteFile(CheckpointPath(5), mismatch);
  auto mismatched = store.Load(5);
  ASSERT_FALSE(mismatched.ok());
  EXPECT_TRUE(mismatched.status().IsCorruption()) << mismatched.status().ToString();
}

}  // namespace
