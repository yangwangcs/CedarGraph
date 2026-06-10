#include <gtest/gtest.h>
#include "cedar/partition/partition_strategy_manager.h"
#include "cedar/partition/strategies/static_hash_strategy.h"

using namespace cedar::partition;

TEST(PartitionStrategyManagerTest, Initialize) {
  PartitionStrategyManager mgr;
  StrategySelectionConfig config;
  config.default_strategy = StrategyType::STATIC_HASH;
  EXPECT_TRUE(mgr.Initialize(config).ok());
}

TEST(PartitionStrategyManagerTest, RegisterStrategy) {
  PartitionStrategyManager mgr;
  StrategySelectionConfig config;
  mgr.Initialize(config);

  auto strategy = std::make_unique<StaticHashStrategy>();
  EXPECT_TRUE(mgr.RegisterStrategy(std::move(strategy)).ok());
}

TEST(PartitionStrategyManagerTest, RegisterNullStrategyFails) {
  PartitionStrategyManager mgr;
  StrategySelectionConfig config;
  mgr.Initialize(config);

  EXPECT_FALSE(mgr.RegisterStrategy(nullptr).ok());
}

TEST(PartitionStrategyManagerTest, SetActiveStrategyByType) {
  PartitionStrategyManager mgr;
  StrategySelectionConfig config;
  mgr.Initialize(config);

  auto strategy = std::make_unique<StaticHashStrategy>();
  ASSERT_TRUE(mgr.RegisterStrategy(std::move(strategy)).ok());

  EXPECT_TRUE(mgr.SetActiveStrategy(StrategyType::STATIC_HASH).ok());
  EXPECT_NE(mgr.GetActiveStrategy(), nullptr);
}

TEST(PartitionStrategyManagerTest, RouteVertex) {
  PartitionStrategyManager mgr;
  StrategySelectionConfig config;
  mgr.Initialize(config);

  auto strategy = std::make_unique<StaticHashStrategy>();
  ASSERT_TRUE(mgr.RegisterStrategy(std::move(strategy)).ok());
  ASSERT_TRUE(mgr.SetActiveStrategy(StrategyType::STATIC_HASH).ok());

  auto result = mgr.RouteVertex(12345);
  EXPECT_GE(result.partition_id, 0);
}

TEST(PartitionStrategyManagerTest, RouteEdge) {
  PartitionStrategyManager mgr;
  StrategySelectionConfig config;
  mgr.Initialize(config);

  auto strategy = std::make_unique<StaticHashStrategy>();
  ASSERT_TRUE(mgr.RegisterStrategy(std::move(strategy)).ok());
  ASSERT_TRUE(mgr.SetActiveStrategy(StrategyType::STATIC_HASH).ok());

  auto result = mgr.RouteEdge(1, 2);
  EXPECT_GE(result.first.partition_id, 0);
  EXPECT_GE(result.second.partition_id, 0);
}

TEST(PartitionStrategyManagerTest, UpdateQueryStats) {
  PartitionStrategyManager mgr;
  StrategySelectionConfig config;
  config.enable_auto_selection = true;
  mgr.Initialize(config);

  auto strategy = std::make_unique<StaticHashStrategy>();
  ASSERT_TRUE(mgr.RegisterStrategy(std::move(strategy)).ok());
  ASSERT_TRUE(mgr.SetActiveStrategy(StrategyType::STATIC_HASH).ok());

  for (int i = 0; i < 200; ++i) {
    mgr.UpdateQueryStats(true, true);
  }
  // Just verify it doesn't crash; auto-switch may or may not trigger
  EXPECT_NE(mgr.GetActiveStrategy(), nullptr);
}

TEST(PartitionStrategyManagerTest, GetAllStats) {
  PartitionStrategyManager mgr;
  StrategySelectionConfig config;
  mgr.Initialize(config);

  auto strategy = std::make_unique<StaticHashStrategy>();
  ASSERT_TRUE(mgr.RegisterStrategy(std::move(strategy)).ok());

  std::string stats = mgr.GetAllStats();
  EXPECT_FALSE(stats.empty());
}
