#include <gtest/gtest.h>
#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/raft/raft_interface.h"

using namespace cedar::dtx;

class MetaServiceTest : public ::testing::Test {
protected:
    void SetUp() override {
        MetaServiceConfig config;
        config.node_id = 1;
        config.listen_address = "127.0.0.1:2379";
        config.advertise_address = "127.0.0.1:2379";
        
        auto status = meta_service_.Initialize(config);
        EXPECT_TRUE(status.ok()) << status.ToString();
    }
    
    void TearDown() override {
        meta_service_.Shutdown();
    }
    
    MetadataService meta_service_;
};

TEST_F(MetaServiceTest, InitializeShutdown) {
    EXPECT_TRUE(meta_service_.IsLeader());
    EXPECT_EQ(meta_service_.GetLeader(), 1);
}

TEST_F(MetaServiceTest, CreateAndGetSpace) {
    SpaceDef space;
    space.name = "test_space";
    space.partition_num = 128;
    space.replica_factor = 3;
    
    auto status = meta_service_.CreateSpace(space);
    EXPECT_TRUE(status.ok()) << status.ToString();
    
    auto result = meta_service_.GetSpace("test_space");
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.value().name, "test_space");
    EXPECT_EQ(result.value().partition_num, 128);
}

TEST_F(MetaServiceTest, GetNonExistentSpace) {
    auto result = meta_service_.GetSpace("non_existent");
    EXPECT_FALSE(result.ok());
}

TEST_F(MetaServiceTest, RegisterNode) {
    NodeInfo node;
    node.node_id = 100;
    node.address = "10.0.0.1:50051";
    node.data_path = "/data/cedar";
    
    auto status = meta_service_.RegisterNode(node);
    EXPECT_TRUE(status.ok()) << status.ToString();
}

TEST_F(MetaServiceTest, GetPartitionAssignmentNotFound) {
    auto result = meta_service_.GetPartitionAssignment("test", 0);
    EXPECT_FALSE(result.ok());
}

TEST(MetaServiceClientTest, Connect) {
    MetaServiceClient client;
    std::vector<std::string> addresses = {"127.0.0.1:2379"};
    auto status = client.Connect(addresses);
    EXPECT_TRUE(status.ok());
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
