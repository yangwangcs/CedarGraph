#include <gtest/gtest.h>
#include "cedar/queryd/distributed_executor.h"
#include "cedar/queryd/meta_client.h"

using namespace cedar;
using namespace cedar::queryd;

// Mock QueryMetaClient that returns test partition data
class MockQueryMetaClient : public QueryMetaClient {
 public:
  MockQueryMetaClient() : QueryMetaClient(Options{}) {}

  Status GetClusterState(ClusterState* state) override {
    state->partition_count = partition_count_;
    state->partitions = partitions_;
    return Status::OK();
  }

  Status GetSchema(GraphSchema* schema) override {
    return Status::OK();
  }

  void SetPartitions(std::vector<PartitionInfo> partitions, uint32_t count) {
    partitions_ = std::move(partitions);
    partition_count_ = count;
  }

 private:
  std::vector<PartitionInfo> partitions_;
  uint32_t partition_count_ = 0;
};

class FollowerReadRoutingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_client_ = std::make_unique<MockQueryMetaClient>();

    // Set up 3 partitions with leader + 2 followers each
    std::vector<PartitionInfo> partitions;
    for (uint32_t pid = 0; pid < 3; ++pid) {
      PartitionInfo info;
      info.partition_id = pid;
      info.leader_address = "node" + std::to_string(pid) + ":9779";
      info.follower_addresses = {
          "follower" + std::to_string(pid) + "a:9779",
          "follower" + std::to_string(pid) + "b:9779"};
      info.is_healthy = true;
      partitions.push_back(info);
    }
    mock_client_->SetPartitions(std::move(partitions), 3);

    router_ = std::make_unique<PartitionRouter>(mock_client_.get());
  }

  std::unique_ptr<MockQueryMetaClient> mock_client_;
  std::unique_ptr<PartitionRouter> router_;
};

// Test: GetFollowerNode returns a valid follower address
TEST_F(FollowerReadRoutingTest, GetFollowerNodeReturnsValidAddress) {
  std::string address;
  Status s = router_->GetFollowerNode(0, &address);
  EXPECT_TRUE(s.ok());
  EXPECT_FALSE(address.empty());
  // Should be one of the followers
  EXPECT_TRUE(address == "follower0a:9779" || address == "follower0b:9779");
}

// Test: GetFollowerNode with exclude_address returns a different follower
TEST_F(FollowerReadRoutingTest, GetFollowerNodeExcludesAddress) {
  std::string addr1;
  Status s = router_->GetFollowerNode(0, &addr1);
  ASSERT_TRUE(s.ok());

  // Get a different follower, excluding the one we just got
  std::string addr2;
  s = router_->GetFollowerNode(0, &addr2, addr1);
  EXPECT_TRUE(s.ok());
  EXPECT_NE(addr1, addr2);
}

// Test: GetFollowerNode exclude returns Unavailable when only 1 follower
TEST_F(FollowerReadRoutingTest, GetFollowerNodeExcludeUnavailableWithSingleFollower) {
  // Set up partition with only 1 follower
  std::vector<PartitionInfo> partitions;
  PartitionInfo info;
  info.partition_id = 99;
  info.leader_address = "leader:9779";
  info.follower_addresses = {"follower1:9779"};
  partitions.push_back(info);
  mock_client_->SetPartitions(std::move(partitions), 1);
  router_ = std::make_unique<PartitionRouter>(mock_client_.get());

  std::string addr1;
  Status s = router_->GetFollowerNode(99, &addr1);
  ASSERT_TRUE(s.ok());

  // Excluding the only follower should fail
  std::string addr2;
  s = router_->GetFollowerNode(99, &addr2, addr1);
  EXPECT_FALSE(s.ok());
}

// Test: GetStorageNode with kEventual routes to follower
TEST_F(FollowerReadRoutingTest, EventualConsistencyRoutesToFollower) {
  std::string address;
  Status s = router_->GetStorageNode(
      0, &address, DistributedExecutionContext::Consistency::kEventual);
  EXPECT_TRUE(s.ok());
  // Should be a follower, not the leader
  EXPECT_NE(address, "node0:9779");
  EXPECT_TRUE(address == "follower0a:9779" || address == "follower0b:9779");
}

// Test: GetStorageNode with kReadYourWrites routes to leader
TEST_F(FollowerReadRoutingTest, ReadYourWritesRoutesToLeader) {
  std::string address;
  Status s = router_->GetStorageNode(
      0, &address, DistributedExecutionContext::Consistency::kReadYourWrites);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(address, "node0:9779");
}

// Test: GetStorageNode with kStrong routes to leader
TEST_F(FollowerReadRoutingTest, StrongConsistencyRoutesToLeader) {
  std::string address;
  Status s = router_->GetStorageNode(
      0, &address, DistributedExecutionContext::Consistency::kStrong);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(address, "node0:9779");
}

// Test: GetStorageNode with kEventual falls back to leader when no followers
TEST_F(FollowerReadRoutingTest, EventualFallsBackToLeaderWhenNoFollowers) {
  std::vector<PartitionInfo> partitions;
  PartitionInfo info;
  info.partition_id = 0;
  info.leader_address = "leader:9779";
  info.follower_addresses = {};  // No followers
  partitions.push_back(info);
  mock_client_->SetPartitions(std::move(partitions), 1);
  router_ = std::make_unique<PartitionRouter>(mock_client_.get());

  std::string address;
  Status s = router_->GetStorageNode(
      0, &address, DistributedExecutionContext::Consistency::kEventual);
  EXPECT_TRUE(s.ok());
  // Should fall back to leader
  EXPECT_EQ(address, "leader:9779");
}

// Test: IsFollowerLagError detects lag errors
TEST_F(FollowerReadRoutingTest, IsFollowerLagErrorDetection) {
  EXPECT_TRUE(DistributedExecutor::IsFollowerLagError(
      Status::Unavailable("Follower lag too high: 1500 entries behind")));
  EXPECT_TRUE(DistributedExecutor::IsFollowerLagError(
      Status::Unavailable("follower lag too high")));
  EXPECT_FALSE(DistributedExecutor::IsFollowerLagError(
      Status::Unavailable("Not leader")));
  EXPECT_FALSE(DistributedExecutor::IsFollowerLagError(
      Status::IOError("Connection refused")));
  EXPECT_FALSE(DistributedExecutor::IsFollowerLagError(Status::OK()));
}

// Test: GetFollowerNode for non-existent partition
TEST_F(FollowerReadRoutingTest, GetFollowerNodeNonExistentPartition) {
  std::string address;
  Status s = router_->GetFollowerNode(999, &address);
  EXPECT_FALSE(s.ok());
}

// Test: Multiple calls to GetFollowerNode can return different followers
TEST_F(FollowerReadRoutingTest, GetFollowerNodeRandomSelection) {
  std::set<std::string> seen;
  for (int i = 0; i < 20; ++i) {
    std::string address;
    Status s = router_->GetFollowerNode(0, &address);
    ASSERT_TRUE(s.ok());
    seen.insert(address);
  }
  // With 20 calls and 2 followers, we should see both at least once
  EXPECT_GE(seen.size(), 1u);
}
