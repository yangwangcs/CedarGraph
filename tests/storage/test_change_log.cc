#include <cstdint>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "cedar/cdc/partition_change_log.h"

namespace {

using cedar::cdc::ChangeRecord;
using cedar::cdc::PartitionChangeLog;

class PartitionChangeLogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("cedar_change_log_test_" +
                 std::to_string(reinterpret_cast<uintptr_t>(this)));
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  PartitionChangeLog::Options Options(uint32_t partition_id = 5,
                                      uint64_t epoch = 9) const {
    PartitionChangeLog::Options options;
    options.directory = test_dir_.string();
    options.partition_id = partition_id;
    options.partition_epoch = epoch;
    options.max_segment_bytes = 180;
    options.max_fetch_records = 4096;
    options.max_fetch_bytes = 4 * 1024 * 1024;
    return options;
  }

  std::unique_ptr<PartitionChangeLog> OpenLog(
      PartitionChangeLog::Options options) const {
    auto result = PartitionChangeLog::Open(std::move(options));
    EXPECT_TRUE(result.ok()) << result.status().ToString();
    if (!result.ok()) {
      return nullptr;
    }
    return std::move(result.ValueOrDie());
  }

  std::vector<ChangeRecord> MakeBatch(size_t count) const {
    std::vector<ChangeRecord> records;
    records.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      ChangeRecord record;
      record.set_txn_id(10 + i);
      record.set_entity_id(100 + i);
      record.set_target_id(200 + i);
      record.set_operation(cedar::cdc::CHANGE_OPERATION_UPDATE);
      record.set_payload(std::string(16, static_cast<char>('a' + i)));
      records.push_back(std::move(record));
    }
    return records;
  }

  std::vector<std::filesystem::path> SegmentFiles() const {
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      if (entry.path().extension() == ".seg") {
        files.push_back(entry.path());
      }
    }
    std::sort(files.begin(), files.end());
    return files;
  }

  void CorruptSecondRecord() const {
    auto segments = SegmentFiles();
    ASSERT_FALSE(segments.empty());
    std::fstream file(segments.front(), std::ios::in | std::ios::out |
                                            std::ios::binary);
    ASSERT_TRUE(file.is_open());
    constexpr std::streamoff kHeaderBytes = 16;
    uint32_t first_payload_size = 0;
    file.seekg(8);
    file.read(reinterpret_cast<char*>(&first_payload_size),
              sizeof(first_payload_size));
    ASSERT_TRUE(file.good());
    file.seekp(kHeaderBytes + first_payload_size + kHeaderBytes + 3);
    char bad = 0x55;
    file.write(&bad, 1);
    ASSERT_TRUE(file.good());
  }

  void InflateSecondRecordPayloadSize() const {
    auto segments = SegmentFiles();
    ASSERT_FALSE(segments.empty());
    std::fstream file(segments.front(), std::ios::in | std::ios::out |
                                            std::ios::binary);
    ASSERT_TRUE(file.is_open());
    constexpr std::streamoff kHeaderBytes = 16;
    uint32_t first_payload_size = 0;
    file.seekg(8);
    file.read(reinterpret_cast<char*>(&first_payload_size),
              sizeof(first_payload_size));
    ASSERT_TRUE(file.good());
    const uint32_t inflated_payload_size = 1 << 20;
    file.seekp(kHeaderBytes + first_payload_size + 8);
    file.write(reinterpret_cast<const char*>(&inflated_payload_size),
               sizeof(inflated_payload_size));
    ASSERT_TRUE(file.good());
  }

  void ReplaceInManifest(const std::string& from, const std::string& to) const {
    const auto path = test_dir_ / "MANIFEST";
    std::ifstream input(path);
    ASSERT_TRUE(input.is_open());
    std::string contents((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
    const auto pos = contents.find(from);
    ASSERT_NE(pos, std::string::npos) << contents;
    contents.replace(pos, from.size(), to);
    std::ofstream output(path, std::ios::trunc);
    ASSERT_TRUE(output.is_open());
    output << contents;
    ASSERT_TRUE(output.good());
  }

  std::filesystem::path test_dir_;
};

TEST_F(PartitionChangeLogTest, ReopensWithContinuousOffsets) {
  auto log = OpenLog(Options());
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(2)).ok());

  log.reset();
  auto reopened = OpenLog(Options());
  ASSERT_NE(reopened, nullptr);
  EXPECT_EQ(reopened->GetState().partition_epoch, 9);
  EXPECT_EQ(reopened->GetState().earliest_offset, 1);
  EXPECT_EQ(reopened->GetState().high_watermark, 2);
  EXPECT_EQ(reopened->GetState().committed_version, 100);

  auto result = reopened->ReadAfter(0, 10, 1 << 20);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  const auto& records = result.ValueOrDie();
  ASSERT_EQ(records.size(), 2);
  EXPECT_EQ(records[0].offset(), 1);
  EXPECT_EQ(records[1].offset(), 2);
  EXPECT_EQ(records[0].partition_id(), 5);
  EXPECT_EQ(records[0].partition_epoch(), 9);
  EXPECT_EQ(records[0].commit_version(), 100);
  EXPECT_EQ(records[0].batch_index(), 0);
  EXPECT_EQ(records[1].batch_index(), 1);
  EXPECT_EQ(records[1].batch_size(), 2);
}

TEST_F(PartitionChangeLogTest, ReadAfterHonorsRecordAndByteLimits) {
  auto log = OpenLog(Options());
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(5)).ok());

  auto by_count = log->ReadAfter(1, 2, 1 << 20);
  ASSERT_TRUE(by_count.ok()) << by_count.status().ToString();
  ASSERT_EQ(by_count.ValueOrDie().size(), 2);
  EXPECT_EQ(by_count.ValueOrDie()[0].offset(), 2);
  EXPECT_EQ(by_count.ValueOrDie()[1].offset(), 3);

  auto by_bytes = log->ReadAfter(0, 10, 1);
  ASSERT_TRUE(by_bytes.ok()) << by_bytes.status().ToString();
  EXPECT_LE(by_bytes.ValueOrDie().size(), 1);
}

TEST_F(PartitionChangeLogTest, RollsSegmentsAndReopensAllRecords) {
  auto options = Options();
  options.max_segment_bytes = 96;
  auto log = OpenLog(options);
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(8)).ok());
  EXPECT_GT(SegmentFiles().size(), 1);

  log.reset();
  auto reopened = OpenLog(options);
  ASSERT_NE(reopened, nullptr);
  auto result = reopened->ReadAfter(0, 20, 1 << 20);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().size(), 8);
  for (size_t i = 0; i < result.ValueOrDie().size(); ++i) {
    EXPECT_EQ(result.ValueOrDie()[i].offset(), i + 1);
  }
}

TEST_F(PartitionChangeLogTest, RecoversIncompleteTailFrame) {
  auto log = OpenLog(Options());
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(3)).ok());
  auto segments = SegmentFiles();
  ASSERT_FALSE(segments.empty());
  const auto old_size = std::filesystem::file_size(segments.back());
  ASSERT_GT(old_size, 8u);
  std::filesystem::resize_file(segments.back(), old_size - 4);

  log.reset();
  auto reopened = OpenLog(Options());
  ASSERT_NE(reopened, nullptr);
  EXPECT_EQ(reopened->GetState().high_watermark, 2);
  auto result = reopened->ReadAfter(0, 10, 1 << 20);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().size(), 2);
}

TEST_F(PartitionChangeLogTest, RejectsMiddleRecordCorruption) {
  auto log = OpenLog(Options());
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(3)).ok());
  CorruptSecondRecord();

  log.reset();
  auto result = PartitionChangeLog::Open(Options());
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsCorruption()) << result.status().ToString();
}

TEST_F(PartitionChangeLogTest, RejectsInflatedMiddlePayloadSize) {
  auto log = OpenLog(Options());
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(3)).ok());
  InflateSecondRecordPayloadSize();

  log.reset();
  auto result = PartitionChangeLog::Open(Options());
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsCorruption()) << result.status().ToString();
}

TEST_F(PartitionChangeLogTest, RejectsManifestHighWatermarkMismatch) {
  auto log = OpenLog(Options());
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(3)).ok());
  ReplaceInManifest("high_watermark 3", "high_watermark 1");

  log.reset();
  auto result = PartitionChangeLog::Open(Options());
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsCorruption()) << result.status().ToString();
}

TEST_F(PartitionChangeLogTest, RejectsManifestPartitionEpochMismatch) {
  auto log = OpenLog(Options());
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(2)).ok());
  ReplaceInManifest("partition_epoch 9", "partition_epoch 8");

  log.reset();
  auto result = PartitionChangeLog::Open(Options());
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsCorruption()) << result.status().ToString();
}

TEST_F(PartitionChangeLogTest, RejectsManifestSegmentRangeMismatch) {
  auto log = OpenLog(Options());
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(2)).ok());
  ReplaceInManifest("first_offset 1", "first_offset 2");

  log.reset();
  auto result = PartitionChangeLog::Open(Options());
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsCorruption()) << result.status().ToString();
}

TEST_F(PartitionChangeLogTest, CompactDropsEarlierOffsets) {
  auto log = OpenLog(Options());
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(5)).ok());
  ASSERT_TRUE(log->Compact(4).ok());
  EXPECT_EQ(log->GetState().earliest_offset, 4);
  EXPECT_EQ(log->GetState().high_watermark, 5);

  log.reset();
  auto reopened = OpenLog(Options());
  ASSERT_NE(reopened, nullptr);
  EXPECT_EQ(reopened->GetState().earliest_offset, 4);
  auto result = reopened->ReadAfter(0, 10, 1 << 20);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().size(), 2);
  EXPECT_EQ(result.ValueOrDie()[0].offset(), 4);
  EXPECT_EQ(result.ValueOrDie()[1].offset(), 5);
}

}  // namespace
