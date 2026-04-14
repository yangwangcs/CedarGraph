#include <gtest/gtest.h>
#include "cedar/dtx/storage/raft_replication.h"

using namespace cedar;
using namespace cedar::dtx;

TEST(RaftProposeBatchTest, EmptyBatchReturnsOk) {
  storage::RaftGroupConfig config;
  config.partition_id = 1;
  config.data_dir = "/tmp/raft_test_propose_batch";
  storage::StorageRaftGroup raft(config);
  std::vector<storage::StorageLogEntry> entries;
  Status s = raft.ProposeBatch(entries);
  EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST(RaftProposeBatchTest, NonEmptyBatchRequiresLeader) {
  storage::RaftGroupConfig config;
  config.partition_id = 1;
  config.data_dir = "/tmp/raft_test_propose_batch2";
  storage::StorageRaftGroup raft(config);
  std::vector<storage::StorageLogEntry> entries(2);
  entries[0].type = storage::StorageLogEntry::Type::kPut;
  entries[1].type = storage::StorageLogEntry::Type::kPut;
  Status s = raft.ProposeBatch(entries);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotLeader());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
