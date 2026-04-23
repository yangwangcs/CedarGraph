#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "cedar/gcn/tmv_index.h"
#include "cedar/gcn/tmv_vertex_entry.h"

using namespace cedar::gcn;

TEST(TMVIndexTest, FindOrCreate) {
  TMVIndex index;
  TMVVertexEntry* entry = index.FindOrCreate(42);
  ASSERT_NE(entry, nullptr);
  EXPECT_EQ(entry->entity_id, 42);
}

TEST(TMVIndexTest, FindExisting) {
  TMVIndex index;
  TMVVertexEntry* created = index.FindOrCreate(42);
  ASSERT_NE(created, nullptr);

  TMVVertexEntry* found = index.Find(42);
  EXPECT_EQ(found, created);
}

TEST(TMVIndexTest, FindMissing) {
  TMVIndex index;
  TMVVertexEntry* found = index.Find(99);
  EXPECT_EQ(found, nullptr);
}

TEST(TMVIndexTest, ShardIsolation) {
  TMVIndex index;
  index.Reserve(1024);

  constexpr int kNumThreads = 8;
  constexpr int kEntriesPerThread = 100;

  std::vector<std::thread> threads;
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < kEntriesPerThread; ++i) {
        uint64_t id = static_cast<uint64_t>(t) * kEntriesPerThread + i;
        TMVVertexEntry* entry = index.FindOrCreate(id);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->entity_id, id);
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  // Verify all entries are findable
  for (int t = 0; t < kNumThreads; ++t) {
    for (int i = 0; i < kEntriesPerThread; ++i) {
      uint64_t id = static_cast<uint64_t>(t) * kEntriesPerThread + i;
      TMVVertexEntry* entry = index.Find(id);
      ASSERT_NE(entry, nullptr);
      EXPECT_EQ(entry->entity_id, id);
    }
  }
}
