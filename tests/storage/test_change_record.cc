#include <string>

#include <gtest/gtest.h>

#include "cdc_service.pb.h"

TEST(ChangeRecordTest, RoundTripsRequiredFields) {
  cedar::cdc::ChangeRecord record;
  record.set_partition_id(7);
  record.set_partition_epoch(3);
  record.set_offset(41);
  record.set_commit_version(99);
  record.set_txn_id(12);
  record.set_batch_index(0);
  record.set_batch_size(1);
  record.set_entity_id(100);
  record.set_target_id(200);
  record.set_operation(cedar::cdc::CHANGE_OPERATION_CREATE);
  record.set_checksum(1234);
  std::string bytes;
  ASSERT_TRUE(record.SerializeToString(&bytes));
  cedar::cdc::ChangeRecord decoded;
  ASSERT_TRUE(decoded.ParseFromString(bytes));
  EXPECT_EQ(decoded.offset(), 41);
  EXPECT_EQ(decoded.target_id(), 200);
}

TEST(ChangeRecordTest, OperationZeroIsUnspecified) {
  cedar::cdc::ChangeRecord record;
  EXPECT_EQ(record.operation(), cedar::cdc::CHANGE_OPERATION_UNSPECIFIED);
}
