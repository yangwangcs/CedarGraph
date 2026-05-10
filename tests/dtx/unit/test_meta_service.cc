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
        config.test_mode = true;

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
    // With BRaftNode, leader ID is derived from listen address port in current implementation
    EXPECT_NE(meta_service_.GetLeader(), kInvalidNodeID);
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

TEST_F(MetaServiceTest, CreateAndGetSchema) {
    SpaceDef space;
    space.name = "test_space";
    space.partition_num = 8;
    space.replica_factor = 1;
    EXPECT_TRUE(meta_service_.CreateSpace(space).ok());

    LabelSchema schema;
    schema.name = "Person";
    schema.properties.push_back({"name", "STRING", true, true});
    schema.properties.push_back({"age", "INT", false, false});
    schema.indexes.push_back("name");

    EXPECT_TRUE(meta_service_.CreateLabelSchema("test_space", schema).ok());

    auto result = meta_service_.GetSchema("test_space", {});
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].name, "Person");
    EXPECT_EQ(result[0].properties.size(), 2);
    EXPECT_EQ(result[0].properties[0].name, "name");
}

TEST_F(MetaServiceTest, GetSchemaFilteredByLabel) {
    SpaceDef space;
    space.name = "test_space";
    space.partition_num = 8;
    space.replica_factor = 1;
    meta_service_.CreateSpace(space);

    LabelSchema s1; s1.name = "A";
    LabelSchema s2; s2.name = "B";
    meta_service_.CreateLabelSchema("test_space", s1);
    meta_service_.CreateLabelSchema("test_space", s2);

    auto result = meta_service_.GetSchema("test_space", {"B"});
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].name, "B");
}

TEST_F(MetaServiceTest, SchemaSurvivesSnapshotRoundtrip) {
    SpaceDef space;
    space.name = "snap_space";
    space.partition_num = 4;
    space.replica_factor = 1;
    meta_service_.CreateSpace(space);

    LabelSchema schema;
    schema.name = "Car";
    schema.properties.push_back({"model", "STRING", true, false});
    meta_service_.CreateLabelSchema("snap_space", schema);

    auto snapshot_data = meta_service_.SerializeState();
    EXPECT_FALSE(snapshot_data.empty());

    MetadataService restored;
    MetaServiceConfig config;
    config.node_id = 2;
    config.listen_address = "127.0.0.1:2380";
    config.advertise_address = "127.0.0.1:2380";
    config.test_mode = true;
    EXPECT_TRUE(restored.Initialize(config).ok());
    EXPECT_TRUE(restored.DeserializeState(snapshot_data));

    auto schemas = restored.GetSchema("snap_space", {});
    EXPECT_EQ(schemas.size(), 1);
    EXPECT_EQ(schemas[0].name, "Car");
    EXPECT_EQ(schemas[0].properties[0].type, "STRING");
    restored.Shutdown();
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
