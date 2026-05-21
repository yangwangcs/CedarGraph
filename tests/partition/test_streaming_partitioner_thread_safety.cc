// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
//
// Ported from subgraph2

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>

#include "cedar/partition/mth/streaming_partitioner.h"
#include "cedar/partition/mth/cedar_key.h"
#include "cedar/partition/mth/op_type.h"

using namespace cedar;
using namespace cedar::partition;

TEST(StreamingPartitionerThreadSafety, ConcurrentAssignEvent) {
  constexpr uint16_t kNumPartitions = 4;
  constexpr size_t kCapacity = 10000;
  constexpr int kNumThreads = 8;
  constexpr int kEventsPerThread = 1000;

  StreamingPartitioner partitioner(kNumPartitions, kCapacity);

  std::atomic<int> total_assigned{0};
  std::vector<std::thread> threads;

  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&partitioner, &total_assigned, t, kNumPartitions]() {
      for (int i = 0; i < kEventsPerThread; ++i) {
        uint64_t entity_id = static_cast<uint64_t>(t) * kEventsPerThread + i;
        CedarKey key = CedarKey::Vertex(entity_id, 0, entity_id);
        uint16_t pid = partitioner.AssignEvent(key);
        EXPECT_LT(pid, kNumPartitions);
        total_assigned.fetch_add(1);
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  EXPECT_EQ(total_assigned.load(), kNumThreads * kEventsPerThread);

  // Verify total events count is consistent
  size_t total_events = 0;
  for (const auto& state : partitioner.states()) {
    total_events += state.EventCount();
  }
  EXPECT_EQ(total_events, static_cast<size_t>(kNumThreads * kEventsPerThread));
}

TEST(StreamingPartitionerThreadSafety, ConcurrentMixedOperations) {
  constexpr uint16_t kNumPartitions = 4;
  constexpr size_t kCapacity = 10000;
  constexpr int kNumThreads = 8;
  constexpr int kOpsPerThread = 500;

  StreamingPartitioner partitioner(kNumPartitions, kCapacity);

  std::vector<std::thread> threads;

  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&partitioner, t, kNumPartitions]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        uint64_t entity_id = static_cast<uint64_t>(t) * kOpsPerThread + i;

        // Mix of create and update operations
        if (i % 3 == 0) {
          CedarKey key = CedarKey::Vertex(entity_id, 0, entity_id,
                                          0, 0, 0, OpType::kUpdate);
          partitioner.AssignEvent(key);
        } else {
          CedarKey key = CedarKey::Vertex(entity_id, 0, entity_id);
          partitioner.AssignEvent(key);
        }

        // Occasionally set migration affinity
        if (i % 10 == 0) {
          std::unordered_map<uint64_t, uint16_t> affinity;
          affinity[entity_id] = static_cast<uint16_t>(entity_id % kNumPartitions);
          partitioner.SetMigrationAffinity(affinity);
        }
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  // Verify total events count is consistent
  size_t total_events = 0;
  for (const auto& state : partitioner.states()) {
    total_events += state.EventCount();
  }
  EXPECT_EQ(total_events, static_cast<size_t>(kNumThreads * kOpsPerThread));
}
