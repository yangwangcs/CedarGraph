// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include <gtest/gtest.h>

#include "cedar/queryd/distributed_executor.h"
#include "cedar/queryd/meta_client.h"
#include "cedar/partition/partition_strategy_manager.h"
#include "cedar/partition/strategies/static_hash_strategy.h"
#include "cedar/partition/strategies/mth_stream_strategy.h"

using namespace cedar;
using namespace cedar::queryd;
using namespace cedar::partition;

class TestMetaClient : public QueryMetaClient {
 public:
  explicit TestMetaClient(const ClusterState& state)
      : QueryMetaClient(Options{}), state_(state) {}

  Status GetClusterState(ClusterState* state) override {
    *state = state_;
    return Status::OK();
  }

 private:
  ClusterState state_;
};

TEST(PartitionRoutingConsistency, StaticHashStrategyDelegation) {
  ClusterState state;
  state.partition_count = 16;

  TestMetaClient meta_client(state);
  PartitionRouter router(&meta_client);

  // Without strategy manager, fallback to modulo
  EXPECT_EQ(router.GetPartitionId(5), 5);
  EXPECT_EQ(router.GetPartitionId(17), 1);

  // Create strategy manager with StaticHashStrategy (16 partitions)
  PartitionStrategyManager manager;
  StrategySelectionConfig config;
  config.default_strategy = StrategyType::STATIC_HASH;
  ASSERT_TRUE(manager.Initialize(config).ok());
  ASSERT_TRUE(manager.RegisterStrategy(
      std::make_unique<StaticHashStrategy>(16)).ok());
  ASSERT_TRUE(manager.SetActiveStrategy(StrategyType::STATIC_HASH).ok());

  router.SetStrategyManager(&manager);

  // With strategy manager, should delegate and get same result
  for (uint64_t id : {0, 1, 5, 16, 17, 100, 12345}) {
    uint32_t from_router = router.GetPartitionId(id);
    uint32_t from_manager = manager.RouteVertex(id).partition_id;
    EXPECT_EQ(from_router, from_manager)
        << "Entity " << id << " routed inconsistently";
  }
}

TEST(PartitionRoutingConsistency, MTHStreamStrategyDelegation) {
  ClusterState state;
  state.partition_count = 16;

  TestMetaClient meta_client(state);
  PartitionRouter router(&meta_client);

  // Create strategy manager with MTHStreamStrategy (16 partitions)
  PartitionStrategyManager manager;
  StrategySelectionConfig config;
  config.default_strategy = StrategyType::MTH_STREAM;
  ASSERT_TRUE(manager.Initialize(config).ok());

  MTHStreamStrategy::Config mth_config;
  mth_config.num_partitions = 16;
  ASSERT_TRUE(manager.RegisterStrategy(
      std::make_unique<MTHStreamStrategy>(mth_config)).ok());
  ASSERT_TRUE(manager.SetActiveStrategy(StrategyType::MTH_STREAM).ok());

  router.SetStrategyManager(&manager);

  // With strategy manager, should delegate and get same result
  for (uint64_t id : {0, 1, 5, 16, 17, 100, 12345}) {
    uint32_t from_router = router.GetPartitionId(id);
    uint32_t from_manager = manager.RouteVertex(id).partition_id;
    EXPECT_EQ(from_router, from_manager)
        << "Entity " << id << " routed inconsistently under MTH";
  }
}

TEST(PartitionRoutingConsistency, SameEntityAlwaysSamePartition) {
  ClusterState state;
  state.partition_count = 16;

  TestMetaClient meta_client(state);
  PartitionRouter router(&meta_client);

  PartitionStrategyManager manager;
  StrategySelectionConfig config;
  config.default_strategy = StrategyType::STATIC_HASH;
  ASSERT_TRUE(manager.Initialize(config).ok());
  ASSERT_TRUE(manager.RegisterStrategy(
      std::make_unique<StaticHashStrategy>(16)).ok());
  ASSERT_TRUE(manager.SetActiveStrategy(StrategyType::STATIC_HASH).ok());

  router.SetStrategyManager(&manager);

  // Same entity should always route to the same partition
  uint64_t entity_id = 42;
  uint32_t first = router.GetPartitionId(entity_id);
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(router.GetPartitionId(entity_id), first);
  }
}
