//===----------------------------------------------------------------------===//
// CedarScan Unit Tests
//===----------------------------------------------------------------------===//

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>

#include "cedar/query/cedar_scan.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/types/descriptor.h"
#include "cedar/core/env.h"

using namespace cedar;

class CedarScanTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/cedar_scan_test_" + 
                std::to_string(
                    std::chrono::system_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir_);
    
    CedarOptions options;
    options.create_if_missing = true;
    options.write_buffer_size = 1024 * 1024;  // 1MB
    
    env_ = cedar::Env::Default();
    engine_ = std::make_unique<LsmEngine>(test_dir_, options, env_);
    ASSERT_TRUE(engine_->Open().ok());
  }
  
  void TearDown() override {
    if (engine_) {
      engine_->Close();
    }
    std::filesystem::remove_all(test_dir_);
  }
  
  std::string test_dir_;
  cedar::Env* env_;
  std::unique_ptr<LsmEngine> engine_;
};

// Test basic CedarScan creation
TEST_F(CedarScanTest, BasicCreation) {
  auto scan = CedarScan::At(Timestamp(1000), engine_.get());
  EXPECT_EQ(scan.snapshot_time().value(), 1000);
  EXPECT_EQ(scan.engine(), engine_.get());
}

// Test GetNode on empty database
TEST_F(CedarScanTest, GetNodeEmpty) {
  auto scan = CedarScan::At(Timestamp(1000), engine_.get());
  auto node = scan.GetNode(12345);
  EXPECT_FALSE(node.has_value());
}

// Test GetNode after writing a vertex
TEST_F(CedarScanTest, GetNodeAfterWrite) {
  // Write a vertex at timestamp 100, column_id = 1
  // Note: column_id should be consistent between CedarKey and Descriptor
  Descriptor desc = Descriptor::InlineInt(1, 42);  // column_id = 1
  CedarKey key = CedarKey::Vertex(12345, 1, Timestamp(100));
  ASSERT_TRUE(engine_->Put(key, desc, Timestamp(1)).ok());
  
  // Query at timestamp 50 (should not see the vertex)
  {
    auto scan = CedarScan::At(Timestamp(50), engine_.get());
    auto node = scan.GetNode(12345);
    EXPECT_FALSE(node.has_value());
  }
  
  // Query at timestamp 100 (should see the vertex)
  {
    auto scan = CedarScan::At(Timestamp(100), engine_.get());
    auto node = scan.GetNode(12345);
    ASSERT_TRUE(node.has_value()) << "GetNode failed at t=100";
    EXPECT_EQ(node->node_id(), 12345);
    EXPECT_EQ(node->timestamp().value(), 100);
  }
  
  // Query at timestamp 200 (should see the vertex - latest version)
  {
    auto scan = CedarScan::At(Timestamp(200), engine_.get());
    auto node = scan.GetNode(12345);
    ASSERT_TRUE(node.has_value()) << "GetNode failed at t=200";
    EXPECT_EQ(node->node_id(), 12345);
  }
}

// Test GetNode with multiple versions
TEST_F(CedarScanTest, GetNodeMultipleVersions) {
  // Write version 1 at timestamp 100
  Descriptor desc1 = Descriptor::InlineInt(0, 1);
  CedarKey key1 = CedarKey::Vertex(12345, 1, Timestamp(100));
  ASSERT_TRUE(engine_->Put(key1, desc1, Timestamp(1)).ok());
  
  // Write version 2 at timestamp 200
  Descriptor desc2 = Descriptor::InlineInt(0, 2);
  CedarKey key2 = CedarKey::Vertex(12345, 1, Timestamp(200));
  ASSERT_TRUE(engine_->Put(key2, desc2, Timestamp(2)).ok());
  
  // Query at timestamp 150 (should see version 1)
  {
    auto scan = CedarScan::At(Timestamp(150), engine_.get());
    auto node = scan.GetNode(12345);
    ASSERT_TRUE(node.has_value());
    EXPECT_EQ(node->timestamp().value(), 100);
  }
  
  // Query at timestamp 250 (should see version 2)
  {
    auto scan = CedarScan::At(Timestamp(250), engine_.get());
    auto node = scan.GetNode(12345);
    ASSERT_TRUE(node.has_value());
    EXPECT_EQ(node->timestamp().value(), 200);
  }
}

// Test GetRecordAtTime returns full CedarKey
TEST_F(CedarScanTest, GetRecordAtTimeReturnsFullKey) {
  // Write a vertex with specific metadata
  Descriptor desc = Descriptor::InlineInt(0, 42);
  CedarKey key = CedarKey::Vertex(12345, 1, Timestamp(100), 5, 10);
  key.SetFlags(key_flags::kHasVInline);
  ASSERT_TRUE(engine_->Put(key, desc, Timestamp(1)).ok());
  
  // Force flush to SST to ensure we read back the full key
  ASSERT_TRUE(engine_->ForceFlush().ok());
  
  // Query using GetRecordAtTime
  auto result = engine_->GetRecordAtTime(12345, EntityType::Vertex, 1, Timestamp(150));
  ASSERT_TRUE(result.has_value());
  
  const auto& [ret_key, ret_desc] = result.value();
  EXPECT_EQ(ret_key.entity_id(), 12345);
  EXPECT_EQ(ret_key.column_id(), 1);
  EXPECT_EQ(ret_key.timestamp().value(), 100);
  EXPECT_EQ(ret_key.sequence(), 5);
  EXPECT_EQ(ret_key.part_id(), 10);
  EXPECT_TRUE(ret_key.HasVInline());
}

// Test ScanEdges basic functionality
TEST_F(CedarScanTest, ScanEdgesBasic) {
  // Write some edges
  Descriptor desc = Descriptor::InlineInt(0, 100);
  
  // Edge: 1 -> 2 (type 1)
  CedarKey edge1 = CedarKey::EdgeOut(1, 2, EdgeTypeId(1), Timestamp(100));
  ASSERT_TRUE(engine_->Put(edge1, desc, Timestamp(1)).ok());
  
  // Edge: 1 -> 3 (type 1)
  CedarKey edge2 = CedarKey::EdgeOut(1, 3, EdgeTypeId(1), Timestamp(100));
  ASSERT_TRUE(engine_->Put(edge2, desc, Timestamp(2)).ok());
  
  // Edge: 1 -> 4 (type 2)
  CedarKey edge3 = CedarKey::EdgeOut(1, 4, EdgeTypeId(2), Timestamp(100));
  ASSERT_TRUE(engine_->Put(edge3, desc, Timestamp(3)).ok());
  
  // Scan all edges from node 1
  auto edges = engine_->ScanEdges(1, EntityType::EdgeOut, 0xFFFF, Timestamp(150));
  
  // Should find 3 edges
  EXPECT_EQ(edges.size(), 3);
  
  // Check targets
  std::vector<uint64_t> targets;
  for (const auto& [target, ts, desc, edge_type] : edges) {
    targets.push_back(target);
  }
  std::sort(targets.begin(), targets.end());
  EXPECT_EQ(targets, std::vector<uint64_t>({2, 3, 4}));
}

// Test ScanEdges with type filter
TEST_F(CedarScanTest, ScanEdgesWithTypeFilter) {
  Descriptor desc = Descriptor::InlineInt(0, 100);
  
  // Edge: 1 -> 2 (type 1)
  CedarKey edge1 = CedarKey::EdgeOut(1, 2, EdgeTypeId(1), Timestamp(100));
  ASSERT_TRUE(engine_->Put(edge1, desc, Timestamp(1)).ok());
  
  // Edge: 1 -> 3 (type 2)
  CedarKey edge2 = CedarKey::EdgeOut(1, 3, EdgeTypeId(2), Timestamp(100));
  ASSERT_TRUE(engine_->Put(edge2, desc, Timestamp(2)).ok());
  
  // Scan only type 1 edges
  auto edges = engine_->ScanEdges(1, EntityType::EdgeOut, 1, Timestamp(150));
  
  EXPECT_EQ(edges.size(), 1);
  EXPECT_EQ(std::get<0>(edges[0]), 2);
  EXPECT_EQ(std::get<3>(edges[0]), 1);  // edge_type
}

// Test NodeView accessors
TEST_F(CedarScanTest, NodeViewAccessors) {
  Descriptor desc = Descriptor::InlineInt(0, 42);
  CedarKey key = CedarKey::Vertex(12345, 5, Timestamp(100), 3, 7, 999);
  key.SetFlags(key_flags::kHasVInline);
  ASSERT_TRUE(engine_->Put(key, desc, Timestamp(1)).ok());
  
  auto scan = CedarScan::At(Timestamp(150), engine_.get());
  auto node = scan.GetNode(12345);
  ASSERT_TRUE(node.has_value());
  
  EXPECT_EQ(node->node_id(), 12345);
  EXPECT_EQ(node->timestamp().value(), 100);
  EXPECT_EQ(node->column_id(), 5);
  EXPECT_EQ(node->sequence(), 3);
  EXPECT_EQ(node->flags(), key_flags::kHasVInline);
  EXPECT_TRUE(node->has_inline_value());
  EXPECT_EQ(node->inline_value(), 999);
  EXPECT_FALSE(node->is_deleted());
}

// Test EdgeView accessors
TEST_F(CedarScanTest, EdgeViewAccessors) {
  Descriptor desc = Descriptor::InlineInt(0, 42);
  CedarKey key = CedarKey::EdgeOut(100, 200, EdgeTypeId(5), Timestamp(100), 3, 7);
  key.SetFlags(key_flags::kHasVInline);
  
  EdgeView view(key, desc, true);
  
  EXPECT_EQ(view.src_id(), 100);
  EXPECT_EQ(view.dst_id(), 200);
  EXPECT_EQ(view.edge_type(), 5);
  EXPECT_EQ(view.timestamp().value(), 100);
  EXPECT_EQ(view.sequence(), 3);
  EXPECT_TRUE(view.is_out_edge());
  EXPECT_FALSE(view.is_in_edge());
  EXPECT_TRUE(view.has_inline_value());
}

// Test EdgeView for InEdges
TEST_F(CedarScanTest, EdgeViewInEdge) {
  Descriptor desc = Descriptor::InlineInt(0, 42);
  CedarKey key = CedarKey::EdgeIn(200, 100, EdgeTypeId(5), Timestamp(100));
  
  EdgeView view(key, desc, false);
  
  // For InEdge: entity_id = dst, target_id = src
  EXPECT_EQ(view.src_id(), 100);
  EXPECT_EQ(view.dst_id(), 200);
  EXPECT_FALSE(view.is_out_edge());
  EXPECT_TRUE(view.is_in_edge());
}

// Test ScanEdges version folding
TEST_F(CedarScanTest, ScanEdgesVersionFolding) {
  Descriptor desc = Descriptor::InlineInt(0, 1);
  
  // Write first version of edge
  CedarKey edge1 = CedarKey::EdgeOut(1, 2, EdgeTypeId(1), Timestamp(100));
  ASSERT_TRUE(engine_->Put(edge1, desc, Timestamp(1)).ok());
  
  // Write second version (update) of same edge
  Descriptor desc2 = Descriptor::InlineInt(0, 2);
  CedarKey edge2 = CedarKey::EdgeOut(1, 2, EdgeTypeId(1), Timestamp(200));
  ASSERT_TRUE(engine_->Put(edge2, desc2, Timestamp(2)).ok());
  
  // Scan should return only one edge (latest version)
  auto edges = engine_->ScanEdges(1, EntityType::EdgeOut, 1, Timestamp(250));
  
  // Should find only 1 edge (version folded)
  EXPECT_EQ(edges.size(), 1);
  
  // Should be the latest version
  EXPECT_EQ(std::get<1>(edges[0]).value(), 200);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
