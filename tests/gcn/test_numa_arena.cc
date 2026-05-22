#include <gtest/gtest.h>

#include "cedar/gcn/numa_arena.h"
#include "cedar/gcn/tmv_chunk.h"

using namespace cedar::gcn;

TEST(ArenaPoolTest, AllocReturnsAlignedChunk) {
  ArenaPool pool(4);
  TMVChunk* chunk = pool.Alloc();
  ASSERT_NE(chunk, nullptr);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(chunk) % 4096, 0);
  EXPECT_EQ(chunk->event_count.load(), 0);
  EXPECT_FALSE(chunk->sealed.load());
  pool.Free(chunk);
}

TEST(ArenaPoolTest, FreeRecyclesChunk) {
  ArenaPool pool(4);
  TMVChunk* a = pool.Alloc();
  TMVChunk* b = pool.Alloc();
  ASSERT_NE(a, nullptr);
  ASSERT_NE(b, nullptr);
  EXPECT_NE(a, b);

  pool.Free(a);
  TMVChunk* c = pool.Alloc();
  EXPECT_EQ(c, a);

  pool.Free(b);
  pool.Free(c);
}

TEST(ArenaPoolTest, ExhaustionReturnsNull) {
  constexpr size_t kPoolSize = 3;
  ArenaPool pool(kPoolSize);
  std::vector<TMVChunk*> chunks;
  for (size_t i = 0; i < kPoolSize; ++i) {
    TMVChunk* chunk = pool.Alloc();
    ASSERT_NE(chunk, nullptr);
    chunks.push_back(chunk);
  }
  TMVChunk* extra = pool.Alloc();
  EXPECT_EQ(extra, nullptr);

  for (TMVChunk* chunk : chunks) {
    pool.Free(chunk);
  }
}
