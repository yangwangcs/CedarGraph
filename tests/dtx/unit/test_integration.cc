#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/dtx/meta_service.h"
#define private public
#include "cedar/dtx/load_balancer.h"
#include "cedar/dtx/migration_executor.h"
#undef private
#include "cedar/dtx/coordinator_integration.h"

using namespace cedar;
using namespace cedar::dtx;

TEST(RouteCacheTest, UpdateGetAndInvalidate) {
    PartitionRouteCache cache;
    
    PartitionRoute route;
    route.partition_id = 0;
    route.leader_node = 100;
    route.version = 1;
    route.cached_at = std::chrono::steady_clock::now();
    
    cache.UpdateRoute("test_space", route);
    
    auto result = cache.GetRoute("test_space", 0);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.value().leader_node, 100);
    
    cache.Invalidate("test_space", 0);
    result = cache.GetRoute("test_space", 0);
    EXPECT_FALSE(result.ok());
    
    cache.UpdateRoute("test_space", route);
    cache.InvalidateAll();
    result = cache.GetRoute("test_space", 0);
    EXPECT_FALSE(result.ok());
}

TEST(IntegrationStub, LoadBalancerCompileAndLink) {
    LoadBalancerConfig config;
    (void)config;
    EXPECT_TRUE(true);
}

TEST(LoadBalancerTest, StopWakesCheckThreadPromptly) {
    LoadBalancerConfig config;
    config.auto_balance_enabled = false;
    config.check_interval_sec = 30;

    LoadBalancer balancer;
    ASSERT_TRUE(balancer.Initialize(config, nullptr,
                                    std::make_unique<LeaderBalanceStrategy>()).ok());
    ASSERT_TRUE(balancer.Start().ok());

    auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(balancer.Stop().ok());
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
              500);
}

TEST(LoadBalancerTest, TerminalFailedTasksDoNotConsumeConcurrencySlots) {
    LoadBalancerConfig config;
    config.auto_balance_enabled = false;
    config.max_concurrent_tasks = 1;

    LoadBalancer balancer;
    ASSERT_TRUE(balancer.Initialize(config, nullptr,
                                    std::make_unique<LeaderBalanceStrategy>()).ok());
    balancer.running_.store(true);

    BalancePlan plan;
    BalanceTask failed_task;
    failed_task.type = static_cast<BalanceTaskType>(255);
    failed_task.partition_id = 1;
    failed_task.source_node = 1;
    failed_task.target_node = 2;
    plan.tasks.push_back(failed_task);

    BalanceTask success_task;
    success_task.type = BalanceTaskType::kAddReplica;
    success_task.partition_id = 2;
    success_task.source_node = 1;
    success_task.target_node = 2;
    plan.tasks.push_back(success_task);

    auto start = std::chrono::steady_clock::now();
    auto status = balancer.ExecuteBalancePlan(plan);
    auto elapsed = std::chrono::steady_clock::now() - start;

    balancer.running_.store(false);

    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
              500);

    auto failed_status = balancer.executor_.GetTaskStatus(1);
    ASSERT_TRUE(failed_status.ok());
    EXPECT_EQ(failed_status.value().state, TaskExecutionState::kFailed);

    EXPECT_FALSE(balancer.executor_.GetTaskStatus(2).ok());
}

TEST(IntegrationStub, CoordinatorCompileAndLink) {
    IntegratedCoordinatorConfig config;
    EXPECT_TRUE(config.meta_addresses.empty() || !config.meta_addresses.empty());
}

TEST(StorageConnectionPoolTest, CloseAllWakesHealthCheckThreadPromptly) {
    StorageConnectionPool pool;

    auto start = std::chrono::steady_clock::now();
    pool.CloseAll();
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 500);
}

TEST(IntegrationStub, MigrationExecutorCompileAndLink) {
    PartitionMigrationExecutor executor;
    (void)executor;
    EXPECT_TRUE(true);
}

TEST(MigrationExecutorTest, RejectsSubmissionsOutsideLifecycle) {
    MigrationExecutor executor;
    MigrationPlan::Action action;
    action.partition_id = 1;
    action.from_node = 1;
    action.to_node = 2;

    EXPECT_EQ(executor.SubmitMigration(action), 0);

    MigrationConfig config;
    config.max_concurrent_migrations = 1;
    MigrationExecutor running_executor(config);
    ASSERT_TRUE(running_executor.Initialize(nullptr, nullptr).ok());
    running_executor.Shutdown();

    EXPECT_EQ(running_executor.SubmitMigration(action), 0);
}

TEST(MigrationExecutorTest, RejectsZeroWorkerConfiguration) {
    MigrationConfig config;
    config.max_concurrent_migrations = 0;
    MigrationExecutor executor(config);

    auto status = executor.Initialize(nullptr, nullptr);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}

TEST(MigrationExecutorTest, FailedExecutionReachesTerminalFailedState) {
    MigrationConfig config;
    config.max_concurrent_migrations = 1;
    MigrationExecutor executor(config);
    ASSERT_TRUE(executor.Initialize(nullptr, nullptr).ok());

    MigrationPlan::Action action;
    action.partition_id = 2;
    action.from_node = 1;
    action.to_node = 2;
    uint64_t id = executor.SubmitMigration(action);
    ASSERT_NE(id, 0u);

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    MigrationProgress progress;
    while (std::chrono::steady_clock::now() < deadline) {
        progress = executor.GetProgress(id);
        if (progress.IsTerminal()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    EXPECT_EQ(progress.state, MigrationState::kFailed)
        << MigrationStateToString(progress.state);
    executor.Shutdown();
}

TEST(MigrationTaskTest, CancelWakesControlWaitPromptly) {
    MigrationConfig config;
    config.enable_dual_write = true;
    config.dual_write_timeout = std::chrono::seconds(30);

    MigrationPlan::Action action;
    action.partition_id = 3;
    action.from_node = 1;
    action.to_node = 2;
    MigrationTask task(1, action, config);

    Status status;
    std::thread worker([&]() {
        status = task.Phase_DualWrite();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto start = std::chrono::steady_clock::now();
    task.Cancel();
    worker.join();
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_FALSE(status.ok());
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
              500);
}

TEST(MigrationTaskTest, ResumeWakesPausedControlWaitPromptly) {
    MigrationConfig config;
    MigrationPlan::Action action;
    action.partition_id = 4;
    action.from_node = 1;
    action.to_node = 2;
    MigrationTask task(2, action, config);

    task.Pause();

    std::atomic<bool> finished{false};
    Status status;
    std::thread worker([&]() {
        status = task.WaitWhilePaused();
        finished.store(true);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(finished.load());

    auto start = std::chrono::steady_clock::now();
    task.Resume();
    worker.join();
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(finished.load());
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
              500);
}
