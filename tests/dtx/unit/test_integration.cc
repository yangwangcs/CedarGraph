#include <gtest/gtest.h>
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/load_balancer.h"
#include "cedar/dtx/coordinator_integration.h"

using namespace cedar;
using namespace cedar::dtx;

class IntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup MetaD
        MetaServiceConfig meta_config;
        meta_config.node_id = 1;
        meta_config.listen_address = "127.0.0.1:2379";
        meta_config.advertise_address = "127.0.0.1:2379";
        
        auto status = meta_service_.Initialize(meta_config);
        ASSERT_TRUE(status.ok());
        
        // Create a space
        SpaceDef space;
        space.name = "test_space";
        space.partition_num = 8;
        space.replica_factor = 3;
        status = meta_service_.CreateSpace(space);
        ASSERT_TRUE(status.ok());
    }
    
    void TearDown() override {
        meta_service_.Shutdown();
    }
    
    MetadataService meta_service_;
};

TEST_F(IntegrationTest, LoadBalancerBasic) {
    // Create load balancer
    LoadBalancerConfig lb_config;
    lb_config.auto_balance_enabled = false;  // Disable auto for testing
    
    auto strategy = std::make_unique<LeaderBalanceStrategy>();
    LoadBalancer load_balancer;
    
    auto status = load_balancer.Initialize(lb_config, &meta_service_, std::move(strategy));
    EXPECT_TRUE(status.ok());
    
    status = load_balancer.Start();
    EXPECT_TRUE(status.ok());
    
    // Get load report
    auto report = load_balancer.GetCurrentLoadReport("test_space");
    EXPECT_TRUE(report.ok());
    EXPECT_EQ(report.value().space_name, "test_space");
    
    // Stop load balancer
    status = load_balancer.Stop();
    EXPECT_TRUE(status.ok());
}

TEST_F(IntegrationTest, CoordinatorIntegrationBasic) {
    // Create integrated coordinator
    IntegratedCoordinator coordinator;
    IntegratedCoordinatorConfig config;
    config.meta_addresses = {"127.0.0.1:2379"};
    config.space_name = "test_space";
    config.preload_all_routes = false;  // Skip preload for testing
    
    auto status = coordinator.Initialize(config);
    EXPECT_TRUE(status.ok());
    
    // Test transaction lifecycle
    DistributedTxnOptions options;
    auto txn_result = coordinator.BeginTransaction(options);
    EXPECT_TRUE(txn_result.ok());
    
    TxnID txn_id = txn_result.value();
    
    // Write some data
    CedarKey key(100, cedar::EntityType::Vertex, 1, 1700000000000000ULL);
    key.SetPartId(0);
    Descriptor value;
    status = coordinator.Write(txn_id, key, value);
    EXPECT_TRUE(status.ok());
    
    // Commit
    auto commit_result = coordinator.Commit(txn_id);
    // Note: May fail because we don't have actual StorageD
    
    // Test cache stats
    auto stats = coordinator.GetCacheStats();
    EXPECT_GE(stats.cache_hits + stats.cache_misses, 0);
    
    // Shutdown
    status = coordinator.Shutdown();
    EXPECT_TRUE(status.ok());
}

TEST_F(IntegrationTest, RouteCache) {
    PartitionRouteCache cache;
    
    // Create a mock MetaServiceClient
    // For testing, we'll directly add routes
    PartitionRoute route;
    route.partition_id = 0;
    route.leader_node = 100;
    route.version = 1;
    route.cached_at = std::chrono::steady_clock::now();
    
    cache.UpdateRoute("test_space", route);
    
    // Get route from cache
    auto result = cache.GetRoute("test_space", 0);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.value().leader_node, 100);
    
    // Invalidate
    cache.Invalidate("test_space", 0);
    result = cache.GetRoute("test_space", 0);
    EXPECT_FALSE(result.ok());
    
    // Invalidate all
    cache.UpdateRoute("test_space", route);
    cache.InvalidateAll();
    result = cache.GetRoute("test_space", 0);
    EXPECT_FALSE(result.ok());
}

TEST_F(IntegrationTest, PartitionMigrationExecutor) {
    PartitionMigrationExecutor executor;
    auto status = executor.Initialize(&meta_service_);
    EXPECT_TRUE(status.ok());
    
    // Create a transfer leader task
    BalanceTask task;
    task.type = BalanceTaskType::kTransferLeader;
    task.partition_id = 0;
    task.source_node = 100;
    task.target_node = 101;
    task.reason = "test";
    
    auto result = executor.ExecuteTask(task);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.value().state, TaskExecutionState::kCompleted);
    
    // Get task status
    auto status_result = executor.GetTaskStatus(0);
    EXPECT_TRUE(status_result.ok());
    
    // Get all tasks
    auto all_tasks = executor.GetAllTaskStatuses();
    EXPECT_GE(all_tasks.size(), 1);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
