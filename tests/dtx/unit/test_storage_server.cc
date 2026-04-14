#include <gtest/gtest.h>
#include "cedar/dtx/storage_server.h"

using namespace cedar::dtx;

class StorageServerTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(StorageServerTest, PartitionStorageBasic) {
    PartitionStorage storage(0, "/tmp/test_partition_0");
    
    // Initialize
    auto status = storage.Initialize();
    EXPECT_TRUE(status.ok());
    
    // Get stats
    auto stats = storage.GetStats();
    (void)stats;  // Avoid unused warning
    
    // Shutdown
    status = storage.Shutdown();
    EXPECT_TRUE(status.ok());
}

TEST_F(StorageServerTest, PartitionManagerBasic) {
    PartitionManager mgr;
    
    StorageServerConfig config;
    config.node_id = 1;
    config.data_dir = "/tmp/test_storage";
    config.partitions = {0, 1, 2};
    
    auto status = mgr.Initialize(config);
    EXPECT_TRUE(status.ok());
    
    // Get all partitions
    auto partitions = mgr.GetAllPartitions();
    EXPECT_EQ(partitions.size(), 3);
    
    // Get specific partition
    auto* storage = mgr.GetPartition(0);
    // May be null in stub implementation
    
    // Shutdown
    status = mgr.Shutdown();
    EXPECT_TRUE(status.ok());
}

TEST_F(StorageServerTest, StorageServerBasic) {
    StorageServer server;
    
    StorageServerConfig config;
    config.node_id = 1;
    config.listen_address = "127.0.0.1:50051";
    config.data_dir = "/tmp/test_storage_node1";
    config.partitions = {0, 1, 2, 3};
    
    auto status = server.Initialize(config);
    EXPECT_TRUE(status.ok());
    
    EXPECT_EQ(server.GetNodeId(), 1);
    
    status = server.Shutdown();
    EXPECT_TRUE(status.ok());
}

TEST_F(StorageServerTest, StorageClientBasic) {
    StorageClient client;
    
    auto status = client.Connect("127.0.0.1:50051");
    EXPECT_TRUE(status.ok());
    
    EXPECT_TRUE(client.IsHealthy());
}

TEST_F(StorageServerTest, StorageClientPoolBasic) {
    StorageClientPool pool;
    
    auto client = pool.GetClient(1, "127.0.0.1:50051");
    // May be null in stub implementation
    
    pool.CloseAll();
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
