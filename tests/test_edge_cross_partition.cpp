// Cross-partition edge scan test using CrossPartitionEdgeWriter
// Two storage instances simulating different partitions.
// Edges: 100->200 (cross-partition), 100->101 (same partition), 200->201 (same partition)
#include <iostream>
#include <filesystem>
#include <set>
#include <functional>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cross_partition_edge_writer.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/types/edge_scan_entry.h"

using namespace cedar;

// Write an edge with proper cross-partition handling:
// EdgeOut goes to src's partition, EdgeIn goes to dst's partition
static Status WriteEdgeCrossPartition(CedarGraphStorage* src_storage,
                                       CedarGraphStorage* dst_storage,
                                       uint64_t src_id, uint64_t dst_id,
                                       uint16_t edge_type, int32_t weight) {
  // In real distributed mode, this would be a 2PC transaction.
  // Here we simulate by writing to the correct partition for each entry.

  // EdgeOut on src's partition
  Descriptor desc = Descriptor::InlineInt(edge_type, weight);
  Status s = src_storage->PutEdge(src_id, dst_id, edge_type,
                                   Timestamp::Now(), desc, Timestamp::Now());
  if (!s.ok()) return s;

  // EdgeIn on dst's partition (if different from src)
  if (src_storage != dst_storage) {
    Descriptor empty_desc = Descriptor::InlineInt(edge_type, 0);
    s = dst_storage->PutEdge(src_id, dst_id, edge_type,
                              Timestamp::Now(), empty_desc, Timestamp::Now());
  }
  return s;
}

// Write an edge within a single partition
static Status WriteEdge(CedarGraphStorage* storage,
                         uint64_t src_id, uint64_t dst_id,
                         uint16_t edge_type, int32_t weight) {
  Descriptor desc = Descriptor::InlineInt(edge_type, weight);
  return storage->PutEdge(src_id, dst_id, edge_type,
                           Timestamp::Now(), desc, Timestamp::Now());
}

// Scan outgoing edges from a node
static std::vector<EdgeScanEntry> ScanOutEdges(CedarGraphStorage* storage,
                                                uint64_t src_id,
                                                uint16_t edge_type) {
  return storage->ScanEdgesWithFolding(src_id, EntityType::EdgeOut,
                                        edge_type, Timestamp::Max());
}

// Scan incoming edges to a node
static std::vector<EdgeScanEntry> ScanInEdges(CedarGraphStorage* storage,
                                               uint64_t dst_id,
                                               uint16_t edge_type) {
  return storage->ScanEdgesWithFolding(dst_id, EntityType::EdgeIn,
                                        edge_type, Timestamp::Max());
}

int main() {
  std::string dir0 = "/tmp/test_edge_partition0";
  std::string dir1 = "/tmp/test_edge_partition1";
  std::filesystem::remove_all(dir0);
  std::filesystem::remove_all(dir1);
  std::filesystem::create_directories(dir0);
  std::filesystem::create_directories(dir1);

  CedarOptions opts;
  opts.create_if_missing = true;

  CedarGraphStorage* storage0 = nullptr;
  CedarGraphStorage* storage1 = nullptr;
  CedarGraphStorage::Open(opts, dir0, &storage0);
  CedarGraphStorage::Open(opts, dir1, &storage1);

  // Setup CrossPartitionEdgeWriter
  std::unordered_map<uint16_t, CedarGraphStorage*> partition_map = {
    {0, storage0},
    {1, storage1}
  };

  auto router = [](uint64_t entity_id) -> uint16_t {
    return static_cast<uint16_t>(entity_id < 150 ? 0 : 1);
  };

  auto storage_accessor = [&partition_map](uint16_t partition_id) -> CedarGraphStorage* {
    auto it = partition_map.find(partition_id);
    return it != partition_map.end() ? it->second : nullptr;
  };

  CrossPartitionEdgeWriter edge_writer(router, storage_accessor);

  constexpr uint16_t KNOWS = 1;    // edge type: KNOWS
  constexpr uint16_t WORKS_AT = 2; // edge type: WORKS_AT

  std::cout << "=== Cross-Partition Edge Scan Test ===" << std::endl;

  // ============================================================
  // Write edges using CrossPartitionEdgeWriter
  // ============================================================
  std::cout << "\n--- Writing edges ---" << std::endl;

  // Intra-partition edges on partition 0: 100->101, 100->102, 101->103
  edge_writer.WriteEdge(100, 101, KNOWS, Descriptor::InlineInt(KNOWS, 5), Timestamp::Now());
  edge_writer.WriteEdge(100, 102, KNOWS, Descriptor::InlineInt(KNOWS, 3), Timestamp::Now());
  edge_writer.WriteEdge(101, 103, KNOWS, Descriptor::InlineInt(KNOWS, 7), Timestamp::Now());
  std::cout << "  partition0: 100->101(KNOWS,5), 100->102(KNOWS,3), 101->103(KNOWS,7)" << std::endl;

  // Intra-partition edges on partition 1: 200->201, 200->202
  edge_writer.WriteEdge(200, 201, KNOWS, Descriptor::InlineInt(KNOWS, 2), Timestamp::Now());
  edge_writer.WriteEdge(200, 202, KNOWS, Descriptor::InlineInt(KNOWS, 4), Timestamp::Now());
  std::cout << "  partition1: 200->201(KNOWS,2), 200->202(KNOWS,4)" << std::endl;

  // Cross-partition edges using the coordinator
  edge_writer.WriteEdge(100, 200, KNOWS, Descriptor::InlineInt(KNOWS, 10), Timestamp::Now());
  edge_writer.WriteEdge(100, 200, WORKS_AT, Descriptor::InlineInt(WORKS_AT, 100), Timestamp::Now());
  std::cout << "  cross-partition (via coordinator): 100->200(KNOWS,10), 100->200(WORKS_AT,100)" << std::endl;

  // Another cross-partition: 200->100 (reverse direction)
  edge_writer.WriteEdge(200, 100, KNOWS, Descriptor::InlineInt(KNOWS, 8), Timestamp::Now());
  std::cout << "  cross-partition (via coordinator): 200->100(KNOWS,8)" << std::endl;

  // Flush both
  storage0->ForceFlush();
  storage1->ForceFlush();
  std::cout << "  Flushed both partitions." << std::endl;

  // ============================================================
  // Test 1: Outgoing edge scan on partition 0
  // ============================================================
  std::cout << "\n--- Test 1: Outgoing edges from node 100 (partition 0) ---" << std::endl;
  {
    auto edges = ScanOutEdges(storage0, 100, KNOWS);
    std::cout << "  Found " << edges.size() << " KNOWS edges:" << std::endl;
    for (const auto& e : edges) {
      std::cout << "    100 -> " << e.target_id
                << " (type=" << e.edge_type
                << ", ts=" << e.timestamp.value() << ")" << std::endl;
    }
  }

  // ============================================================
  // Test 2: Incoming edge scan on partition 1 (dst=200)
  // Node 200 has incoming edge from 100 (cross-partition)
  // The EdgeIn entry should be stored on partition 1
  // ============================================================
  std::cout << "\n--- Test 2: Incoming edges to node 200 (partition 1) ---" << std::endl;
  {
    auto edges = ScanInEdges(storage1, 200, KNOWS);
    std::cout << "  Found " << edges.size() << " incoming KNOWS edges:" << std::endl;
    for (const auto& e : edges) {
      std::cout << "    " << e.target_id << " -> 200"
                << " (type=" << e.edge_type << ")" << std::endl;
    }
  }

  // ============================================================
  // Test 3: Cross-partition traversal simulation
  // Query: "Find all nodes reachable from 100 via KNOWS"
  // Requires: scan EdgeOut on p0 for 100, then scan EdgeOut on p1 for targets
  // ============================================================
  std::cout << "\n--- Test 3: Cross-partition BFS from node 100 ---" << std::endl;
  {
    // Hop 1: scan outgoing edges from 100 on partition 0
    auto hop1_edges = ScanOutEdges(storage0, 100, KNOWS);
    std::set<uint64_t> hop1_targets;
    for (const auto& e : hop1_edges) {
      hop1_targets.insert(e.target_id);
    }
    std::cout << "  Hop 1 from 100: ";
    for (auto id : hop1_targets) std::cout << id << " ";
    std::cout << "(" << hop1_targets.size() << " nodes)" << std::endl;

    // Hop 2: for each target, scan outgoing edges
    // Targets 101, 102 are on partition 0; target 200 is on partition 1
    std::set<uint64_t> hop2_targets;
    for (uint64_t target : hop1_targets) {
      CedarGraphStorage* target_storage = nullptr;
      if (target < 150) {
        target_storage = storage0;  // partition 0
      } else {
        target_storage = storage1;  // partition 1
      }
      auto edges = ScanOutEdges(target_storage, target, KNOWS);
      for (const auto& e : edges) {
        if (e.target_id != 100) {  // avoid going back to start
          hop2_targets.insert(e.target_id);
        }
      }
    }
    std::cout << "  Hop 2: ";
    for (auto id : hop2_targets) std::cout << id << " ";
    std::cout << "(" << hop2_targets.size() << " nodes)" << std::endl;

    // All reachable nodes
    std::set<uint64_t> all_reachable;
    all_reachable.insert(hop1_targets.begin(), hop1_targets.end());
    all_reachable.insert(hop2_targets.begin(), hop2_targets.end());
    std::cout << "  All reachable from 100: ";
    for (auto id : all_reachable) std::cout << id << " ";
    std::cout << std::endl;
  }

  // ============================================================
  // Test 4: Edge type filtering
  // ============================================================
  std::cout << "\n--- Test 4: Edge type filtering ---" << std::endl;
  {
    auto knows = ScanOutEdges(storage0, 100, KNOWS);
    auto works = ScanOutEdges(storage0, 100, WORKS_AT);
    std::cout << "  100 KNOWS edges: " << knows.size() << std::endl;
    std::cout << "  100 WORKS_AT edges: " << works.size() << std::endl;
  }

  // ============================================================
  // Test 5: Reverse edge scan (incoming)
  // ============================================================
  std::cout << "\n--- Test 5: Reverse edge scan (who knows 200?) ---" << std::endl;
  {
    // EdgeIn for node 200 should be on partition 1 (stored by dst_id)
    auto incoming = ScanInEdges(storage1, 200, KNOWS);
    std::cout << "  Incoming KNOWS to 200: " << incoming.size() << " edges" << std::endl;
    for (const auto& e : incoming) {
      std::cout << "    " << e.target_id << " -> 200" << std::endl;
    }
  }

  // ============================================================
  // Verification
  // ============================================================
  std::cout << "\n--- Verification ---" << std::endl;
  bool ok = true;

  // Test 1: node 100 should have 3 outgoing KNOWS edges (101, 102, 200)
  {
    auto edges = ScanOutEdges(storage0, 100, KNOWS);
    std::set<uint64_t> targets;
    for (const auto& e : edges) targets.insert(e.target_id);
    if (targets.count(101) && targets.count(102) && targets.count(200)) {
      std::cout << "  ✅ 100->101, 100->102, 100->200 (outgoing KNOWS)" << std::endl;
    } else {
      std::cout << "  ❌ Missing outgoing edges from 100" << std::endl;
      ok = false;
    }
  }

  // Test 2: node 100 should have 1 outgoing WORKS_AT edge (200)
  {
    auto edges = ScanOutEdges(storage0, 100, WORKS_AT);
    if (edges.size() == 1 && edges[0].target_id == 200) {
      std::cout << "  ✅ 100->200 (outgoing WORKS_AT)" << std::endl;
    } else {
      std::cout << "  ❌ Missing WORKS_AT edge from 100" << std::endl;
      ok = false;
    }
  }

  // Test 3: node 200 should have incoming edge from 100 on partition 1
  {
    auto incoming = ScanInEdges(storage1, 200, KNOWS);
    bool found = false;
    for (const auto& e : incoming) {
      if (e.target_id == 100) found = true;
    }
    if (found) {
      std::cout << "  ✅ 100->200 (incoming KNOWS on partition 1)" << std::endl;
    } else {
      std::cout << "  ❌ Missing incoming edge 100->200 on partition 1" << std::endl;
      ok = false;
    }
  }

  // Test 4: node 100 should have incoming edge from 200 on partition 0
  {
    auto incoming = ScanInEdges(storage0, 100, KNOWS);
    bool found = false;
    for (const auto& e : incoming) {
      if (e.target_id == 200) found = true;
    }
    if (found) {
      std::cout << "  ✅ 200->100 (incoming KNOWS on partition 0)" << std::endl;
    } else {
      std::cout << "  ❌ Missing incoming edge 200->100 on partition 0" << std::endl;
      ok = false;
    }
  }

  // Test 5: cross-partition BFS found nodes on both partitions
  {
    auto hop1 = ScanOutEdges(storage0, 100, KNOWS);
    std::set<uint64_t> targets;
    for (const auto& e : hop1) targets.insert(e.target_id);

    bool has_local = targets.count(101) || targets.count(102);
    bool has_remote = targets.count(200);
    if (has_local && has_remote) {
      std::cout << "  ✅ BFS hop 1 reaches both local (101/102) and remote (200) nodes" << std::endl;
    } else {
      std::cout << "  ❌ BFS hop 1 missing nodes" << std::endl;
      ok = false;
    }
  }

  // Test 6: edge type filtering works
  {
    auto knows = ScanOutEdges(storage0, 100, KNOWS);
    auto works = ScanOutEdges(storage0, 100, WORKS_AT);
    if (knows.size() == 3 && works.size() == 1) {
      std::cout << "  ✅ Edge type filtering: 3 KNOWS + 1 WORKS_AT" << std::endl;
    } else {
      std::cout << "  ❌ Edge type filtering: got " << knows.size()
                << " KNOWS + " << works.size() << " WORKS_AT" << std::endl;
      ok = false;
    }
  }

  std::cout << "\n" << (ok ? "🎉 ALL PASSED" : "❌ FAILED") << std::endl;

  delete storage0;
  delete storage1;
  return ok ? 0 : 1;
}
