#include <gtest/gtest.h>

#include <limits>

#include "cedar/gcn/event_applier.h"
#include "cedar/gcn/tmv_engine.h"
#include "cdc_service.pb.h"

namespace {

cedar::cdc::ChangeRecord MakeEdgeRecord(uint32_t partition_id,
                                        uint64_t offset,
                                        uint64_t entity_id,
                                        uint64_t target_id,
                                        cedar::cdc::ChangeOperation op =
                                            cedar::cdc::CHANGE_OPERATION_CREATE,
                                        uint64_t commit_version = 100) {
  cedar::cdc::ChangeRecord record;
  record.set_partition_id(partition_id);
  record.set_partition_epoch(7);
  record.set_offset(offset);
  record.set_commit_version(commit_version);
  record.set_txn_id(1000 + offset);
  record.set_batch_index(0);
  record.set_batch_size(1);
  record.set_entity_id(entity_id);
  record.set_target_id(target_id);
  record.set_edge_type(2);
  record.set_operation(op);
  record.set_valid_from(commit_version);
  return record;
}

TEST(EventApplierOffsetTest, AppliesCreateAndAdvancesPartitionProgress) {
  cedar::gcn::TMVEngine engine(16);
  cedar::gcn::EventApplier applier(&engine);

  auto record = MakeEdgeRecord(3, 1, 10, 20,
                               cedar::cdc::CHANGE_OPERATION_CREATE, 100);
  auto status = applier.ApplyChangeRecord(record);

  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(applier.AppliedOffset(3), 1u);
  EXPECT_EQ(applier.AppliedVersion(3), 100u);
  auto edges = engine.ScanAtTime(10, cedar::gcn::Direction::kOut, 100);
  ASSERT_EQ(edges.size(), 1u);
  EXPECT_EQ(edges[0].target_id, 20u);
  EXPECT_EQ(edges[0].edge_type, 2u);
}

TEST(EventApplierOffsetTest, DuplicateOffsetIsNoOp) {
  cedar::gcn::TMVEngine engine(16);
  cedar::gcn::EventApplier applier(&engine);
  auto record = MakeEdgeRecord(3, 1, 10, 20,
                               cedar::cdc::CHANGE_OPERATION_CREATE, 100);

  ASSERT_TRUE(applier.ApplyChangeRecord(record).ok());
  ASSERT_TRUE(applier.ApplyChangeRecord(record).ok());

  EXPECT_EQ(applier.AppliedOffset(3), 1u);
  auto edges = engine.ScanAtTime(10, cedar::gcn::Direction::kOut, 100);
  EXPECT_EQ(edges.size(), 1u);
}

TEST(EventApplierOffsetTest, RejectsOffsetGapWithoutMutation) {
  cedar::gcn::TMVEngine engine(16);
  cedar::gcn::EventApplier applier(&engine);
  auto record = MakeEdgeRecord(3, 2, 10, 20,
                               cedar::cdc::CHANGE_OPERATION_CREATE, 100);

  auto status = applier.ApplyChangeRecord(record);

  EXPECT_TRUE(status.IsConflict()) << status.ToString();
  EXPECT_EQ(applier.AppliedOffset(3), 0u);
  EXPECT_TRUE(engine.ScanAtTime(10, cedar::gcn::Direction::kOut, 100).empty());
}

TEST(EventApplierOffsetTest, TracksPartitionsIndependently) {
  cedar::gcn::TMVEngine engine(16);
  cedar::gcn::EventApplier applier(&engine);

  ASSERT_TRUE(applier.ApplyChangeRecord(MakeEdgeRecord(3, 1, 10, 20,
                                                       cedar::cdc::CHANGE_OPERATION_CREATE,
                                                       100))
                  .ok());
  ASSERT_TRUE(applier.ApplyChangeRecord(MakeEdgeRecord(4, 1, 11, 21,
                                                       cedar::cdc::CHANGE_OPERATION_CREATE,
                                                       200))
                  .ok());

  EXPECT_EQ(applier.AppliedOffset(3), 1u);
  EXPECT_EQ(applier.AppliedVersion(3), 100u);
  EXPECT_EQ(applier.AppliedOffset(4), 1u);
  EXPECT_EQ(applier.AppliedVersion(4), 200u);
}

TEST(EventApplierOffsetTest, DeleteRecordRemovesEdgeAtDeleteTime) {
  cedar::gcn::TMVEngine engine(16);
  cedar::gcn::EventApplier applier(&engine);

  ASSERT_TRUE(applier.ApplyChangeRecord(MakeEdgeRecord(3, 1, 10, 20,
                                                       cedar::cdc::CHANGE_OPERATION_CREATE,
                                                       100))
                  .ok());
  ASSERT_TRUE(applier.ApplyChangeRecord(MakeEdgeRecord(3, 2, 10, 20,
                                                       cedar::cdc::CHANGE_OPERATION_DELETE,
                                                       200))
                  .ok());

  EXPECT_EQ(engine.ScanAtTime(10, cedar::gcn::Direction::kOut, 150).size(),
            1u);
  EXPECT_TRUE(engine.ScanAtTime(10, cedar::gcn::Direction::kOut, 250).empty());
  EXPECT_EQ(applier.AppliedOffset(3), 2u);
  EXPECT_EQ(applier.AppliedVersion(3), 200u);
}

TEST(EventApplierOffsetTest, RejectsUnsupportedOperationWithoutProgress) {
  cedar::gcn::TMVEngine engine(16);
  cedar::gcn::EventApplier applier(&engine);
  auto record = MakeEdgeRecord(3, 1, 10, 20,
                               cedar::cdc::CHANGE_OPERATION_UNSPECIFIED, 100);

  auto status = applier.ApplyChangeRecord(record);

  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
  EXPECT_EQ(applier.AppliedOffset(3), 0u);
  EXPECT_TRUE(engine.ScanAtTime(10, cedar::gcn::Direction::kOut, 100).empty());
}

TEST(EventApplierOffsetTest, RejectsInvalidBatchMetadataWithoutProgress) {
  cedar::gcn::TMVEngine engine(16);
  cedar::gcn::EventApplier applier(&engine);
  auto record = MakeEdgeRecord(3, 1, 10, 20,
                               cedar::cdc::CHANGE_OPERATION_CREATE, 100);
  record.set_batch_size(0);

  auto status = applier.ApplyChangeRecord(record);

  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
  EXPECT_EQ(applier.AppliedOffset(3), 0u);
}

TEST(EventApplierOffsetTest, MultiRecordBatchDoesNotMutateUntilComplete) {
  cedar::gcn::TMVEngine engine(16);
  cedar::gcn::EventApplier applier(&engine);
  auto first = MakeEdgeRecord(3, 1, 10, 20,
                              cedar::cdc::CHANGE_OPERATION_CREATE, 100);
  first.set_txn_id(77);
  first.set_batch_index(0);
  first.set_batch_size(2);

  ASSERT_TRUE(applier.ApplyChangeRecord(first).ok());

  EXPECT_EQ(applier.AppliedOffset(3), 0u);
  EXPECT_TRUE(engine.ScanAtTime(10, cedar::gcn::Direction::kOut, 100).empty());
}

TEST(EventApplierOffsetTest, CompleteMultiRecordBatchAppliesAtomically) {
  cedar::gcn::TMVEngine engine(16);
  cedar::gcn::EventApplier applier(&engine);
  auto first = MakeEdgeRecord(3, 1, 10, 20,
                              cedar::cdc::CHANGE_OPERATION_CREATE, 100);
  first.set_txn_id(77);
  first.set_batch_index(0);
  first.set_batch_size(2);
  auto second = MakeEdgeRecord(3, 2, 10, 30,
                               cedar::cdc::CHANGE_OPERATION_CREATE, 100);
  second.set_txn_id(77);
  second.set_batch_index(1);
  second.set_batch_size(2);

  ASSERT_TRUE(applier.ApplyChangeRecord(first).ok());
  ASSERT_TRUE(applier.ApplyChangeRecord(second).ok());

  EXPECT_EQ(applier.AppliedOffset(3), 2u);
  EXPECT_EQ(applier.AppliedVersion(3), 100u);
  auto edges = engine.ScanAtTime(10, cedar::gcn::Direction::kOut, 100);
  ASSERT_EQ(edges.size(), 2u);
  EXPECT_EQ(edges[0].target_id, 20u);
  EXPECT_EQ(edges[1].target_id, 30u);
}

TEST(EventApplierOffsetTest, InvalidMultiRecordBatchDoesNotMutate) {
  cedar::gcn::TMVEngine engine(16);
  cedar::gcn::EventApplier applier(&engine);
  auto first = MakeEdgeRecord(3, 1, 10, 20,
                              cedar::cdc::CHANGE_OPERATION_CREATE, 100);
  first.set_txn_id(77);
  first.set_batch_index(0);
  first.set_batch_size(2);
  auto second = MakeEdgeRecord(3, 2, 10, 30,
                               cedar::cdc::CHANGE_OPERATION_UNSPECIFIED, 100);
  second.set_txn_id(77);
  second.set_batch_index(1);
  second.set_batch_size(2);

  ASSERT_TRUE(applier.ApplyChangeRecord(first).ok());
  auto status = applier.ApplyChangeRecord(second);

  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
  EXPECT_EQ(applier.AppliedOffset(3), 0u);
  EXPECT_TRUE(engine.ScanAtTime(10, cedar::gcn::Direction::kOut, 100).empty());
}

TEST(EventApplierOffsetTest, BatchApplyFailureDoesNotLeavePartialMutation) {
  cedar::gcn::TMVEngine engine(3);
  cedar::gcn::EventApplier applier(&engine);
  auto first = MakeEdgeRecord(3, 1, 10, 20,
                              cedar::cdc::CHANGE_OPERATION_CREATE, 100);
  first.set_txn_id(77);
  first.set_batch_index(0);
  first.set_batch_size(2);
  auto second = MakeEdgeRecord(3, 2, 11, 30,
                               cedar::cdc::CHANGE_OPERATION_CREATE, 100);
  second.set_txn_id(77);
  second.set_batch_index(1);
  second.set_batch_size(2);

  ASSERT_TRUE(applier.ApplyChangeRecord(first).ok());
  auto status = applier.ApplyChangeRecord(second);

  EXPECT_TRUE(status.IsResourceExhausted()) << status.ToString();
  EXPECT_EQ(applier.AppliedOffset(3), 0u);
  EXPECT_EQ(applier.AppliedVersion(3), 0u);
  EXPECT_EQ(engine.VertexCount(), 0u);
  EXPECT_EQ(engine.ChunkCount(), 0u);
  EXPECT_TRUE(engine.ScanAtTime(10, cedar::gcn::Direction::kOut, 100).empty());
  EXPECT_TRUE(engine.ScanAtTime(11, cedar::gcn::Direction::kOut, 100).empty());
  EXPECT_TRUE(engine.ScanAtTime(20, cedar::gcn::Direction::kIn, 100).empty());
  EXPECT_TRUE(engine.ScanAtTime(30, cedar::gcn::Direction::kIn, 100).empty());
}

TEST(EventApplierOffsetTest,
     SnapshotReplacePreservesOtherPartitionReverseEdgesOnSharedTarget) {
  cedar::gcn::TMVEngine engine(32);
  cedar::gcn::EventApplier applier(&engine);

  ASSERT_TRUE(applier.ApplyChangeRecord(MakeEdgeRecord(3, 1, 10, 100,
                                                       cedar::cdc::CHANGE_OPERATION_CREATE,
                                                       100))
                  .ok());
  ASSERT_TRUE(applier.ApplyChangeRecord(MakeEdgeRecord(4, 1, 20, 100,
                                                       cedar::cdc::CHANGE_OPERATION_CREATE,
                                                       101))
                  .ok());

  std::vector<cedar::cdc::ChangeRecord> snapshot_records = {
      MakeEdgeRecord(3, 5, 10, 300, cedar::cdc::CHANGE_OPERATION_CREATE, 500)};
  ASSERT_TRUE(
      applier.ApplySnapshotRecordsAtomically(3, 5, 500, snapshot_records).ok());

  auto shared_target_reverse =
      engine.ScanAtTime(100, cedar::gcn::Direction::kIn, 500);
  ASSERT_EQ(shared_target_reverse.size(), 1u);
  EXPECT_EQ(shared_target_reverse[0].target_id, 20u);
  auto partition_b_reverse =
      engine.ScanAtTime(100, cedar::gcn::Direction::kIn, 101);
  ASSERT_EQ(partition_b_reverse.size(), 1u);
  EXPECT_EQ(partition_b_reverse[0].target_id, 20u);
  auto replacement_reverse =
      engine.ScanAtTime(300, cedar::gcn::Direction::kIn, 500);
  ASSERT_EQ(replacement_reverse.size(), 1u);
  EXPECT_EQ(replacement_reverse[0].target_id, 10u);
}

TEST(EventApplierOffsetTest,
     SnapshotReplaceRemovesUntrackedStalePartitionEdgesAlreadyInTmv) {
  cedar::gcn::TMVEngine engine(32);
  cedar::gcn::EventApplier applier(&engine);

  cedar::gcn::TMVEdge stale{};
  stale.target_id = 100;
  stale.valid_from = 100;
  stale.valid_to = std::numeric_limits<uint32_t>::max();
  stale.edge_type = 2;
  stale.reserved = 3;
  ASSERT_TRUE(engine.AppendEdge(10, cedar::gcn::Direction::kOut, stale, true)
                  .ok());

  std::vector<cedar::cdc::ChangeRecord> snapshot_records = {
      MakeEdgeRecord(3, 5, 11, 101, cedar::cdc::CHANGE_OPERATION_CREATE, 500)};
  ASSERT_TRUE(
      applier.ApplySnapshotRecordsAtomically(3, 5, 500, snapshot_records).ok());

  EXPECT_TRUE(engine.ScanAtTime(10, cedar::gcn::Direction::kOut, 500).empty());
  EXPECT_TRUE(engine.ScanAtTime(100, cedar::gcn::Direction::kIn, 500).empty());
  EXPECT_FALSE(engine.ScanAtTime(11, cedar::gcn::Direction::kOut, 500).empty());
}

}  // namespace
