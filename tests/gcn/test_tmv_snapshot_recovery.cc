#include <gtest/gtest.h>

#include <filesystem>
#include <limits>
#include <vector>

#include "cedar/gcn/tmv_engine.h"
#include "cedar/gcn/tmv_snapshot_store.h"

namespace cedar::gcn {
namespace {

std::filesystem::path MakeTempDir(const char* name) {
  auto dir = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  return dir;
}

void SeedEngine(TMVEngine* engine, uint32_t partition_id) {
  ASSERT_TRUE(engine->AppendEdge(
                        42, Direction::kOut,
                        TMVEdge{100, 10, std::numeric_limits<uint32_t>::max(),
                                11, 1, partition_id},
                        true)
                  .ok());
  ASSERT_TRUE(engine->AppendEdge(
                        42, Direction::kOut,
                        TMVEdge{200, 20, std::numeric_limits<uint32_t>::max(),
                                22, 2, partition_id},
                        true)
                  .ok());
  ASSERT_TRUE(engine->AppendEdge(
                        7, Direction::kOut,
                        TMVEdge{300, 15, std::numeric_limits<uint32_t>::max(),
                                33, 3, partition_id + 1},
                        true)
                  .ok());
}

TEST(TmvSnapshotRecoveryTest, RestoresEdgesAndCheckpointIdentity) {
  const auto dir = MakeTempDir("cedar-tmv-snapshot-recovery-test");
  TmvSnapshotStore store(dir.string());
  TMVEngine engine(16);
  SeedEngine(&engine, /*partition_id=*/3);

  auto save_status = store.SavePartition(engine, /*partition_id=*/3,
                                         /*applied_version=*/100,
                                         /*applied_offset=*/20);
  ASSERT_TRUE(save_status.ok()) << save_status.ToString();

  TMVEngine restored(16);
  auto restored_metadata = store.RestorePartition(&restored, /*partition_id=*/3);
  ASSERT_TRUE(restored_metadata.ok()) << restored_metadata.status().ToString();
  EXPECT_EQ(restored_metadata.ValueOrDie().partition_id, 3u);
  EXPECT_EQ(restored_metadata.ValueOrDie().applied_version, 100u);
  EXPECT_EQ(restored_metadata.ValueOrDie().applied_offset, 20u);

  auto out_edges = restored.ScanAtTime(42, Direction::kOut, 100);
  ASSERT_EQ(out_edges.size(), 2u);
  EXPECT_EQ(out_edges[0].target_id, 100u);
  EXPECT_EQ(out_edges[0].reserved, 3u);
  EXPECT_EQ(out_edges[1].target_id, 200u);
  EXPECT_EQ(out_edges[1].reserved, 3u);

  auto in_edges = restored.ScanAtTime(100, Direction::kIn, 100);
  ASSERT_EQ(in_edges.size(), 1u);
  EXPECT_EQ(in_edges[0].target_id, 42u);
  EXPECT_EQ(in_edges[0].reserved, 3u);

  EXPECT_TRUE(restored.ScanAtTime(7, Direction::kOut, 100).empty());
  std::filesystem::remove_all(dir);
}

TEST(TmvSnapshotRecoveryTest, RejectsCorruptSnapshotWithoutChangingEngine) {
  const auto dir = MakeTempDir("cedar-tmv-snapshot-corrupt-test");
  TmvSnapshotStore store(dir.string());
  TMVEngine engine(16);
  SeedEngine(&engine, /*partition_id=*/3);
  ASSERT_TRUE(store.SavePartition(engine, /*partition_id=*/3,
                                  /*applied_version=*/100,
                                  /*applied_offset=*/20)
                  .ok());

  const auto snapshot_file = store.SnapshotPathForTest(/*partition_id=*/3);
  ASSERT_TRUE(std::filesystem::exists(snapshot_file));
  std::filesystem::resize_file(snapshot_file, 24);

  TMVEngine restored(16);
  ASSERT_TRUE(restored.AppendEdge(
                        99, Direction::kOut,
                        TMVEdge{199, 1, std::numeric_limits<uint32_t>::max(),
                                0, 1, 9},
                        false)
                  .ok());
  auto status = store.RestorePartition(&restored, /*partition_id=*/3);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.status().IsCorruption());

  auto preserved = restored.ScanAtTime(99, Direction::kOut, 2);
  ASSERT_EQ(preserved.size(), 1u);
  EXPECT_EQ(preserved[0].target_id, 199u);
  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace cedar::gcn
