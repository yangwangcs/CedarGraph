#include <gtest/gtest.h>
#include "cedar/queryd/distributed_executor.h"

using namespace cedar;
using namespace cedar::queryd;

class MockMetaClient : public QueryMetaClient {
 public:
  MockMetaClient() : QueryMetaClient(Options{}) {}
  Status GetClusterState(ClusterState* state) override {
    PartitionInfo p1;
    p1.partition_id = 0;
    p1.leader_address = "127.0.0.1:9779";
    p1.follower_addresses = {"127.0.0.1:9780", "127.0.0.1:9781"};
    state->partition_count = 2;
    state->partitions = {p1};
    return Status::OK();
  }
  Status GetSchema(GraphSchema*) override { return Status::OK(); }
};

TEST(PartitionRouterLeaderOnlyTest, ReturnsLeaderAddress) {
  MockMetaClient meta;
  PartitionRouter router(&meta);
  std::string addr;
  Status s = router.GetStorageNode(0, &addr);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(addr, "127.0.0.1:9779");
}

TEST(PartitionRouterLeaderOnlyTest, RejectsFollowerAddress) {
  MockMetaClient meta;
  PartitionRouter router(&meta);
  Status s = router.CheckIsLeader(0, "127.0.0.1:9780");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotLeader());
}

TEST(PartitionRouterLeaderOnlyTest, AllowsAnyWhenDisabled) {
  MockMetaClient meta;
  PartitionRouter router(&meta);
  router.SetRequireLeaderOnly(false);
  Status s = router.CheckIsLeader(0, "127.0.0.1:9780");
  EXPECT_TRUE(s.ok()) << s.ToString();
}
