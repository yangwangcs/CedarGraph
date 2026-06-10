#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <vector>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"
#include "query/query_cache.h"
#include "query_service.pb.h"

using namespace cedar;

// ── Test 1: Cache prefix invalidation ────────────────────────
TEST(CacheAtomicity, QueryCachePrefixInvalidation) {
  cedar::query::QueryCacheConfig config;
  config.max_entries = 100;
  config.max_memory_bytes = 1024 * 1024;
  config.default_ttl_seconds = 60;
  cedar::query::QueryCache cache(config);

  cedar::query::CacheKey key1;
  key1.query_fingerprint = "MATCH (n {id: 10}) RETURN n|p=1";
  key1.partition_hash = 0;
  key1.as_of_timestamp = 0;

  cedar::query::CacheKey key2;
  key2.query_fingerprint = "MATCH (n {id: 10}) RETURN n|p=2";
  key2.partition_hash = 0;
  key2.as_of_timestamp = 0;

  cedar::query::CacheKey key3;
  key3.query_fingerprint = "MATCH (n {id: 20}) RETURN n|p=1";
  key3.partition_hash = 0;
  key3.as_of_timestamp = 0;

  cedar::query::ResultSet rs1;
  rs1.set_total_rows(1);
  cedar::query::ResultSet rs2;
  rs2.set_total_rows(1);
  cedar::query::ResultSet rs3;
  rs3.set_total_rows(1);

  cache.Put(key1, rs1);
  cache.Put(key2, rs2);
  cache.Put(key3, rs3);

  // Invalidate by prefix
  cache.InvalidateByPrefix("MATCH (n {id: 10}) RETURN n");

  // key1 and key2 should be gone
  EXPECT_FALSE(cache.Get(key1).ok());
  EXPECT_FALSE(cache.Get(key2).ok());

  // key3 should still exist
  EXPECT_TRUE(cache.Get(key3).ok());
}

// ── Test 2: Concurrent writes to storage are durable ─────────
TEST(TwoPCAtomicity, ConcurrentWriteDurable) {
  std::string test_dir = "/tmp/cedar_test_ts_mono_" + std::to_string(getpid());
  cedar::CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  auto status = CedarGraphStorage::Open(options, test_dir, &storage);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_NE(storage, nullptr);

  std::atomic<int> success_count{0};

  auto writer = [&](int id) {
    auto desc = cedar::Descriptor::InlineInt(1, static_cast<int32_t>(42 + id));
    auto s = storage->PutStaticVertex(id + 1, 1, desc);
    if (s.ok()) {
      success_count++;
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back(writer, i);
  }
  for (auto& t : threads) t.join();

  EXPECT_EQ(success_count, 10);

  // Verify all writes are readable
  for (int i = 0; i < 10; ++i) {
    auto result = storage->GetStaticVertex(i + 1, 1);
    ASSERT_TRUE(result.has_value()) << "Vertex " << (i + 1) << " not found";
    auto int_val = result->AsInlineInt();
    ASSERT_TRUE(int_val.has_value());
    EXPECT_EQ(int_val.value(), static_cast<int32_t>(42 + i));
  }

  delete storage;
  CedarGraphStorage::DestroyDB(test_dir, options);
}

// ── Test 3: Storage put/get consistency ─────────────────────
TEST(TwoPCAtomicity, StoragePutGetConsistency) {
  std::string test_dir = "/tmp/cedar_test_storage_" + std::to_string(getpid());
  cedar::CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  auto status = CedarGraphStorage::Open(options, test_dir, &storage);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_NE(storage, nullptr);

  // Write
  auto desc = cedar::Descriptor::InlineInt(1, 12345);
  auto s = storage->PutStaticVertex(100, 1, desc);
  ASSERT_TRUE(s.ok());

  // Read back
  auto result = storage->GetStaticVertex(100, 1);
  ASSERT_TRUE(result.has_value());
  auto int_val = result->AsInlineInt();
  ASSERT_TRUE(int_val.has_value());
  EXPECT_EQ(int_val.value(), 12345);

  // Update
  auto desc2 = cedar::Descriptor::InlineInt(1, 99999);
  s = storage->PutStaticVertex(100, 1, desc2);
  ASSERT_TRUE(s.ok());

  // Read latest
  result = storage->GetStaticVertex(100, 1);
  ASSERT_TRUE(result.has_value());
  int_val = result->AsInlineInt();
  ASSERT_TRUE(int_val.has_value());
  EXPECT_EQ(int_val.value(), 99999);

  delete storage;
  CedarGraphStorage::DestroyDB(test_dir, options);
}

// ── Test 4: Sync write durability flag ──────────────────────
TEST(TwoPCAtomicity, SyncWriteOption) {
  std::string test_dir = "/tmp/cedar_test_sync_" + std::to_string(getpid());
  cedar::CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  auto status = CedarGraphStorage::Open(options, test_dir, &storage);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_NE(storage, nullptr);

  cedar::WriteOptions write_options;
  write_options.sync = true;

  auto desc = cedar::Descriptor::InlineInt(1, 55555);
  auto s = storage->Put(write_options, 200, 1000000, desc, Timestamp(1000000));
  ASSERT_TRUE(s.ok());

  // Use Get with the specific column to find the value
  auto result = storage->Get(200, EntityType::Vertex, 1, Timestamp(1000000));
  ASSERT_TRUE(result.has_value());
  auto int_val = result->AsInlineInt();
  ASSERT_TRUE(int_val.has_value());
  EXPECT_EQ(int_val.value(), 55555);

  delete storage;
  CedarGraphStorage::DestroyDB(test_dir, options);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
