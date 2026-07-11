#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "cedar/cdc/change_log_maintenance.h"
#include "cedar/cdc/partition_change_log.h"
#include "cedar/dtx/storage/metrics_collector.h"

namespace {

using cedar::cdc::ChangeLogMaintenance;
using cedar::cdc::ChangeRecord;
using cedar::cdc::PartitionChangeLog;
using cedar::dtx::storage::MetricsCollector;
using cedar::dtx::storage::MetricsRegistry;

class ChangeLogMaintenanceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("cedar_change_log_maintenance_test_" +
                 std::to_string(reinterpret_cast<uintptr_t>(this)));
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
    MetricsRegistry::Instance().Clear();
  }

  void TearDown() override {
    MetricsRegistry::Instance().Clear();
    std::filesystem::remove_all(test_dir_);
  }

  PartitionChangeLog::Options Options() const {
    PartitionChangeLog::Options options;
    options.directory = test_dir_.string();
    options.partition_id = 3;
    options.partition_epoch = 7;
    options.max_segment_bytes = 96;
    options.max_fetch_records = 4096;
    options.max_fetch_bytes = 4 * 1024 * 1024;
    return options;
  }

  std::unique_ptr<PartitionChangeLog> OpenLog() const {
    auto result = PartitionChangeLog::Open(Options());
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
      record.set_txn_id(100 + i);
      record.set_entity_id(1000 + i);
      record.set_target_id(2000 + i);
      record.set_operation(cedar::cdc::CHANGE_OPERATION_UPDATE);
      record.set_payload(std::string(16, static_cast<char>('a' + i)));
      records.push_back(std::move(record));
    }
    return records;
  }

  void AgeClosedSegments(const PartitionChangeLog& log,
                         std::chrono::hours age) const {
    const auto state = log.GetState();
    const auto active_name = SegmentName(state.active_segment_first_offset);
    const auto old_time = std::filesystem::file_time_type::clock::now() - age;
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      if (!entry.is_regular_file() || entry.path().extension() != ".seg" ||
          entry.path().filename().string() == active_name) {
        continue;
      }
      std::filesystem::last_write_time(entry.path(), old_time);
    }
  }

  void AgeFirstClosedSegment(const PartitionChangeLog& log,
                             std::chrono::hours age) const {
    const auto state = log.GetState();
    const auto active_name = SegmentName(state.active_segment_first_offset);
    std::vector<std::filesystem::path> closed_segments;
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      if (entry.is_regular_file() && entry.path().extension() == ".seg" &&
          entry.path().filename().string() != active_name) {
        closed_segments.push_back(entry.path());
      }
    }
    std::sort(closed_segments.begin(), closed_segments.end());
    ASSERT_FALSE(closed_segments.empty());
    std::filesystem::last_write_time(
        closed_segments.front(),
        std::filesystem::file_time_type::clock::now() - age);
  }

  std::string SegmentName(uint64_t first_offset) const {
    std::ostringstream out;
    out << std::setw(20) << std::setfill('0') << first_offset << ".seg";
    return out.str();
  }

  std::filesystem::path test_dir_;
};

TEST_F(ChangeLogMaintenanceTest, RejectsInvalidRetentionConfig) {
  ChangeLogMaintenance::Config zero_retention;
  zero_retention.min_retention_hours = 0;
  EXPECT_TRUE(ChangeLogMaintenance::ValidateConfig(zero_retention)
                  .IsInvalidArgument());

  ChangeLogMaintenance::Config zero_bytes;
  zero_bytes.max_retained_bytes = 0;
  EXPECT_TRUE(ChangeLogMaintenance::ValidateConfig(zero_bytes)
                  .IsInvalidArgument());

  ChangeLogMaintenance::Config zero_interval;
  zero_interval.maintenance_interval = std::chrono::milliseconds(0);
  EXPECT_TRUE(ChangeLogMaintenance::ValidateConfig(zero_interval)
                  .IsInvalidArgument());

  ChangeLogMaintenance::Config huge_bytes;
  huge_bytes.max_rpc_bytes = std::numeric_limits<uint64_t>::max();
  EXPECT_TRUE(ChangeLogMaintenance::ValidateConfig(huge_bytes)
                  .IsInvalidArgument());

  ChangeLogMaintenance::Config huge_records;
  huge_records.max_rpc_records = std::numeric_limits<uint32_t>::max();
  EXPECT_TRUE(ChangeLogMaintenance::ValidateConfig(huge_records)
                  .IsInvalidArgument());
}

TEST_F(ChangeLogMaintenanceTest, ParsesYamlUnsignedConfigStrictly) {
  auto parsed = cedar::cdc::ParseUnsignedConfigValue(
      "storaged.cdc.max_rpc_bytes", "1048576");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  EXPECT_EQ(parsed.ValueOrDie(), 1048576u);

  EXPECT_TRUE(cedar::cdc::ParseUnsignedConfigValue(
                  "storaged.cdc.max_rpc_bytes", "-1")
                  .status()
                  .IsInvalidArgument());
  EXPECT_TRUE(cedar::cdc::ParseUnsignedConfigValue(
                  "storaged.cdc.max_rpc_bytes", "1048576x")
                  .status()
                  .IsInvalidArgument());
  EXPECT_TRUE(cedar::cdc::ParseUnsignedConfigValue(
                  "storaged.cdc.max_rpc_bytes", "")
                  .status()
                  .IsInvalidArgument());
  EXPECT_TRUE(cedar::cdc::ParseUnsignedConfigValue(
                  "storaged.cdc.max_rpc_bytes",
                  "18446744073709551616")
                  .status()
                  .IsInvalidArgument());
}

TEST_F(ChangeLogMaintenanceTest,
       CompactsWhenRetainedBytesExceededButKeepsActiveSegmentReadable) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(8)).ok());
  AgeClosedSegments(*log, std::chrono::hours(48));
  const auto before = log->GetState();
  ASSERT_GT(before.segment_count, 1u);
  ASSERT_GT(before.segment_bytes, 0u);

  ChangeLogMaintenance::Config config;
  config.min_retention_hours = 24;
  config.max_retained_bytes = 1;
  ChangeLogMaintenance maintenance(config);
  ASSERT_TRUE(maintenance.RunOnce(*log).ok());

  const auto after = log->GetState();
  EXPECT_GT(after.earliest_offset, before.earliest_offset);
  EXPECT_LE(after.earliest_offset, after.active_segment_first_offset);
  EXPECT_EQ(after.high_watermark, before.high_watermark);
  EXPECT_EQ(after.committed_version, before.committed_version);

  auto records = log->ReadAfter(after.earliest_offset - 1, 10, 1 << 20);
  ASSERT_TRUE(records.ok()) << records.status().ToString();
  ASSERT_FALSE(records.ValueOrDie().empty());
  EXPECT_EQ(records.ValueOrDie().front().offset(), after.earliest_offset);
  EXPECT_EQ(records.ValueOrDie().back().offset(), after.high_watermark);
}

TEST_F(ChangeLogMaintenanceTest, RepeatedCompactionDoesNotTrimActiveSegment) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(8)).ok());
  AgeClosedSegments(*log, std::chrono::hours(48));

  ChangeLogMaintenance::Config config;
  config.min_retention_hours = 24;
  config.max_retained_bytes = 1;
  ChangeLogMaintenance maintenance(config);
  ASSERT_TRUE(maintenance.RunOnce(*log).ok());
  const auto after_first = log->GetState();

  ASSERT_TRUE(maintenance.RunOnce(*log).ok());
  const auto after_second = log->GetState();

  EXPECT_EQ(after_second.earliest_offset, after_first.earliest_offset);
  EXPECT_EQ(after_second.active_segment_first_offset,
            after_first.active_segment_first_offset);
  EXPECT_EQ(after_second.high_watermark, after_first.high_watermark);
}

TEST_F(ChangeLogMaintenanceTest, DoesNotCompactYoungClosedSegments) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  ASSERT_TRUE(log->AppendCommittedBatch(100, MakeBatch(20)).ok());
  AgeFirstClosedSegment(*log, std::chrono::hours(48));
  const auto before = log->GetState();
  ASSERT_GT(before.segment_count, 2u);

  ChangeLogMaintenance::Config config;
  config.min_retention_hours = 24;
  config.max_retained_bytes = 1;
  ChangeLogMaintenance maintenance(config);
  ASSERT_TRUE(maintenance.RunOnce(*log).ok());
  const auto after = log->GetState();

  EXPECT_EQ(after.earliest_offset, before.earliest_offset);
  EXPECT_EQ(after.high_watermark, before.high_watermark);
}

TEST_F(ChangeLogMaintenanceTest, PeriodicMaintenanceStopsPromptly) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);

  ChangeLogMaintenance::Config config;
  config.maintenance_interval = std::chrono::seconds(60);
  ChangeLogMaintenance maintenance(config);
  ASSERT_TRUE(maintenance.Start([&]() -> std::vector<PartitionChangeLog*> {
    return {log.get()};
  }).ok());

  auto start = std::chrono::steady_clock::now();
  maintenance.Stop();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
}

TEST_F(ChangeLogMaintenanceTest, PublishesPerPartitionCdcMetrics) {
  MetricsCollector collector;
  MetricsCollector::CdcPartitionMetrics metrics;
  metrics.state.partition_epoch = 7;
  metrics.state.earliest_offset = 2;
  metrics.state.high_watermark = 8;
  metrics.state.committed_version = 123;
  metrics.state.segment_bytes = 456;
  metrics.state.segment_count = 3;
  metrics.append_latency_seconds = 0.004;
  metrics.fetch_latency_seconds = 0.006;
  metrics.has_append_latency = true;
  metrics.has_fetch_latency = true;
  metrics.stale_epoch_total = 5;
  metrics.checksum_failures_total = 2;

  collector.RecordCdcPartitionMetrics(3, metrics);
  const std::string text = MetricsRegistry::Instance().ExportPrometheus();

  EXPECT_NE(text.find("cedar_cdc_high_watermark{partition=\"3\"} 8"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("cedar_cdc_earliest_offset{partition=\"3\"} 2"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("cedar_cdc_committed_version{partition=\"3\"} 123"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("cedar_cdc_segment_bytes{partition=\"3\"} 456"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("cedar_cdc_segment_count{partition=\"3\"} 3"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("cedar_cdc_stale_epoch_total{partition=\"3\"} 5"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("cedar_cdc_checksum_failures_total{partition=\"3\"} 2"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("cedar_cdc_append_latency_seconds"), std::string::npos)
      << text;
  EXPECT_NE(text.find("cedar_cdc_fetch_latency_seconds"), std::string::npos)
      << text;
}

TEST_F(ChangeLogMaintenanceTest, CdcTotalMetricsDoNotOvercountOnRepublish) {
  MetricsCollector collector;
  MetricsCollector::CdcPartitionMetrics metrics;
  metrics.state.partition_epoch = 7;
  metrics.state.earliest_offset = 2;
  metrics.state.high_watermark = 8;
  metrics.stale_epoch_total = 5;
  metrics.checksum_failures_total = 2;

  collector.RecordCdcPartitionMetrics(3, metrics);
  collector.RecordCdcPartitionMetrics(3, metrics);
  const std::string text = MetricsRegistry::Instance().ExportPrometheus();

  EXPECT_NE(text.find("cedar_cdc_stale_epoch_total{partition=\"3\"} 5"),
            std::string::npos)
      << text;
  EXPECT_EQ(text.find("cedar_cdc_stale_epoch_total{partition=\"3\"} 10"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("cedar_cdc_checksum_failures_total{partition=\"3\"} 2"),
            std::string::npos)
      << text;
}

TEST_F(ChangeLogMaintenanceTest, CdcLatencyMetricsRequireRealObservation) {
  MetricsCollector collector;
  MetricsCollector::CdcPartitionMetrics metrics;
  metrics.state.partition_epoch = 7;
  metrics.state.earliest_offset = 2;
  metrics.state.high_watermark = 8;

  collector.RecordCdcPartitionMetrics(3, metrics);
  std::string text = MetricsRegistry::Instance().ExportPrometheus();
  EXPECT_EQ(text.find("cedar_cdc_append_latency_seconds"), std::string::npos)
      << text;
  EXPECT_EQ(text.find("cedar_cdc_fetch_latency_seconds"), std::string::npos)
      << text;

  metrics.append_latency_seconds = 0.004;
  metrics.fetch_latency_seconds = 0.006;
  metrics.has_append_latency = true;
  metrics.has_fetch_latency = true;
  collector.RecordCdcPartitionMetrics(3, metrics);
  text = MetricsRegistry::Instance().ExportPrometheus();
  EXPECT_NE(text.find("cedar_cdc_append_latency_seconds"), std::string::npos)
      << text;
  EXPECT_NE(text.find("cedar_cdc_fetch_latency_seconds"), std::string::npos)
      << text;
}

}  // namespace
