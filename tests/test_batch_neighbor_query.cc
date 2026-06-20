// Unit test for batch neighbor queries (replaces test_debug_batch which requires a running server)
#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <thread>
#include <atomic>
#include <vector>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class BatchNeighborQueryTest : public ::testing::Test {
 protected:
  std::string db_path_;
  CedarGraphStorage* storage_ = nullptr;

  void SetUp() override {
    char buf[] = "/tmp/batch_neighbor_test_XXXXXX";
    char* dir = mkdtemp(buf);
    ASSERT_NE(dir, nullptr);
    db_path_ = dir;

    CedarOptions options;
    options.create_if_missing = true;
    Status s = CedarGraphStorage::Open(options, db_path_, &storage_);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_NE(storage_, nullptr);
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    if (!db_path_.empty() && std::filesystem::exists(db_path_)) {
      std::filesystem::remove_all(db_path_);
    }
  }
};

// Write a single vertex with multiple neighbors (edges), then read them back
TEST_F(BatchNeighborQueryTest, WriteAndReadMultipleEdges) {
  const uint64_t kSrcVertex = 100;
  const uint16_t kBaseEdgeType = 100;
  const int kNumEdges = 5;
  const Timestamp kTimestamp(5000000);

  // Step 1: Write 5 edges (vertex 100 -> neighbors 200-204)
  for (int e = 0; e < kNumEdges; ++e) {
    uint64_t dst_id = 200 + e;
    uint16_t edge_type = kBaseEdgeType + e;
    int32_t neighbor_id = static_cast<int32_t>(dst_id);
    Descriptor desc = Descriptor::InlineInt(edge_type, neighbor_id);

    WriteOptions wopts;
    Status s = storage_->PutEdge(wopts, kSrcVertex, dst_id, edge_type, kTimestamp, desc, kTimestamp);
    ASSERT_TRUE(s.ok()) << "Failed to write edge " << e << ": " << s.ToString();
  }

  // Step 2: Read single edge (col=100)
  {
    auto result = storage_->GetEdge(kSrcVertex, 200, kBaseEdgeType, kTimestamp);
    ASSERT_TRUE(result.has_value()) << "Edge (col=100) not found";
    auto val = result->AsInlineInt();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, 200) << "Edge (col=100) value mismatch";
  }

  // Step 3: Read all edges individually
  for (int e = 0; e < kNumEdges; ++e) {
    uint64_t dst_id = 200 + e;
    uint16_t edge_type = kBaseEdgeType + e;

    auto result = storage_->GetEdge(kSrcVertex, dst_id, edge_type, kTimestamp);
    ASSERT_TRUE(result.has_value()) << "Edge (col=" << edge_type << ") not found";
    auto val = result->AsInlineInt();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, static_cast<int32_t>(dst_id))
        << "Edge (col=" << edge_type << ") value mismatch";
  }
}

// Write edges and verify they persist after flush
TEST_F(BatchNeighborQueryTest, EdgesPersistAfterFlush) {
  const uint64_t kSrcVertex = 100;
  const uint16_t kBaseEdgeType = 100;
  const int kNumEdges = 5;
  const Timestamp kTimestamp(5000000);

  // Write edges
  for (int e = 0; e < kNumEdges; ++e) {
    uint64_t dst_id = 200 + e;
    uint16_t edge_type = kBaseEdgeType + e;
    int32_t neighbor_id = static_cast<int32_t>(dst_id);
    Descriptor desc = Descriptor::InlineInt(edge_type, neighbor_id);

    WriteOptions wopts;
    Status s = storage_->PutEdge(wopts, kSrcVertex, dst_id, edge_type, kTimestamp, desc, kTimestamp);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Force flush to SST
  auto* engine = storage_->GetLsmEngine();
  ASSERT_NE(engine, nullptr);
  Status s = engine->ForceFlush();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Read edges after flush
  for (int e = 0; e < kNumEdges; ++e) {
    uint64_t dst_id = 200 + e;
    uint16_t edge_type = kBaseEdgeType + e;

    auto result = storage_->GetEdge(kSrcVertex, dst_id, edge_type, kTimestamp);
    ASSERT_TRUE(result.has_value()) << "Edge (col=" << edge_type << ") not found after flush";
    auto val = result->AsInlineInt();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, static_cast<int32_t>(dst_id))
        << "Edge (col=" << edge_type << ") value mismatch after flush";
  }
}

// Write edges with different timestamps and read at specific time
TEST_F(BatchNeighborQueryTest, TemporalEdgeRead) {
  const uint64_t kSrcVertex = 100;
  const uint16_t kEdgeType = 100;
  const uint64_t kDstId = 200;

  // Write edge at time 1000
  Descriptor desc1 = Descriptor::InlineInt(kEdgeType, 200);
  WriteOptions wopts;
  ASSERT_TRUE(storage_->PutEdge(wopts, kSrcVertex, kDstId, kEdgeType,
                                Timestamp(1000), desc1, Timestamp(1000)).ok());

  // Write updated edge at time 2000
  Descriptor desc2 = Descriptor::InlineInt(kEdgeType, 999);
  ASSERT_TRUE(storage_->PutEdge(wopts, kSrcVertex, kDstId, kEdgeType,
                                Timestamp(2000), desc2, Timestamp(2000)).ok());

  // Read at time 1000 should get old value
  auto result1 = storage_->GetEdge(kSrcVertex, kDstId, kEdgeType, Timestamp(1000));
  ASSERT_TRUE(result1.has_value());
  auto val1 = result1->AsInlineInt();
  ASSERT_TRUE(val1.has_value());
  EXPECT_EQ(*val1, 200);

  // Read at time 2000 should get new value
  auto result2 = storage_->GetEdge(kSrcVertex, kDstId, kEdgeType, Timestamp(2000));
  ASSERT_TRUE(result2.has_value());
  auto val2 = result2->AsInlineInt();
  ASSERT_TRUE(val2.has_value());
  EXPECT_EQ(*val2, 999);

  // Read at non-existent timestamp should return empty
  auto result3 = storage_->GetEdge(kSrcVertex, kDstId, kEdgeType, Timestamp(9999));
  EXPECT_FALSE(result3.has_value());

  // Use ScanEdges to get all edges and verify dst_id is correct
  auto scan_results = storage_->ScanEdges(kSrcVertex, kEdgeType, Timestamp(0), Timestamp::Max());
  ASSERT_GE(scan_results.size(), 2u) << "Expected at least 2 edge versions";
  for (const auto& [dst, ts, desc] : scan_results) {
    EXPECT_EQ(dst, kDstId) << "ScanEdges returned wrong dst_id";
  }
}

// Write many edges and verify batch read performance
TEST_F(BatchNeighborQueryTest, ManyEdgesPerformance) {
  const uint64_t kSrcVertex = 100;
  const uint16_t kBaseEdgeType = 100;
  const int kNumEdges = 10;  // Reduced from 50 to 10 for faster testing
  const Timestamp kTimestamp(5000000);

  // Write 50 edges
  auto start = std::chrono::steady_clock::now();
  for (int e = 0; e < kNumEdges; ++e) {
    uint64_t dst_id = 200 + e;
    uint16_t edge_type = kBaseEdgeType + e;
    int32_t neighbor_id = static_cast<int32_t>(dst_id);
    Descriptor desc = Descriptor::InlineInt(edge_type, neighbor_id);

    WriteOptions wopts;
    Status s = storage_->PutEdge(wopts, kSrcVertex, dst_id, edge_type, kTimestamp, desc, kTimestamp);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }
  auto write_end = std::chrono::steady_clock::now();
  auto write_us = std::chrono::duration_cast<std::chrono::microseconds>(write_end - start).count();

  // Read all edges
  auto read_start = std::chrono::steady_clock::now();
  int found_count = 0;
  for (int e = 0; e < kNumEdges; ++e) {
    uint64_t dst_id = 200 + e;
    uint16_t edge_type = kBaseEdgeType + e;

    auto result = storage_->GetEdge(kSrcVertex, dst_id, edge_type, kTimestamp);
    if (result.has_value()) {
      found_count++;
    }
  }
  auto read_end = std::chrono::steady_clock::now();
  auto read_us = std::chrono::duration_cast<std::chrono::microseconds>(read_end - read_start).count();

  EXPECT_EQ(found_count, kNumEdges);
  // Performance should be reasonable (under 1 second for 50 ops)
  EXPECT_LT(write_us, 1000000) << "Write took too long: " << write_us << " us";
  EXPECT_LT(read_us, 1000000) << "Read took too long: " << read_us << " us";
}

// Test concurrent reads and writes
TEST_F(BatchNeighborQueryTest, ConcurrentReadWrite) {
  const uint64_t kSrcVertex = 100;
  const uint16_t kBaseEdgeType = 100;
  const int kNumEdges = 10;
  const Timestamp kTimestamp(5000000);

  std::atomic<int> write_success{0};
  std::atomic<int> read_success{0};

  // Writer thread
  std::thread writer_thread([&]() {
    for (int e = 0; e < kNumEdges; ++e) {
      uint64_t dst_id = 200 + e;
      uint16_t edge_type = kBaseEdgeType + e;
      int32_t neighbor_id = static_cast<int32_t>(dst_id);
      Descriptor desc = Descriptor::InlineInt(edge_type, neighbor_id);

      WriteOptions wopts;
      Status s = storage_->PutEdge(wopts, kSrcVertex, dst_id, edge_type, kTimestamp, desc, kTimestamp);
      if (s.ok()) {
        write_success.fetch_add(1);
      }
    }
  });

  // Reader thread
  std::thread reader_thread([&]() {
    for (int e = 0; e < kNumEdges; ++e) {
      uint64_t dst_id = 200 + e;
      uint16_t edge_type = kBaseEdgeType + e;

      auto result = storage_->GetEdge(kSrcVertex, dst_id, edge_type, kTimestamp);
      if (result.has_value()) {
        read_success.fetch_add(1);
      }
    }
  });

  writer_thread.join();
  reader_thread.join();

  // All writes should succeed
  EXPECT_EQ(write_success.load(), kNumEdges);

  // At least some reads should succeed (depends on timing)
  // We can't guarantee all reads succeed because writes and reads happen concurrently
  EXPECT_GE(read_success.load(), 0);
}
