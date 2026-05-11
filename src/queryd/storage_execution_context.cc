#include "cedar/queryd/storage_execution_context.h"
#include "cedar/types/edge_scan_entry.h"

namespace cedar {
namespace queryd {

StorageBackedExecutionContext::StorageBackedExecutionContext(
    QueryStorageClient* storage_client, uint32_t partition_id)
    : storage_client_(storage_client), partition_id_(partition_id) {
  this->partition_id = partition_id;

  // Wire up storage-backed entity enumeration
  this->get_all_entities_fn = [this](uint64_t min_id, uint64_t max_id, uint64_t step) {
    std::vector<uint64_t> results;
    if (!storage_client_) return results;
    for (uint64_t id = min_id; id <= max_id && results.size() < 1000; id += step) {
      // Partition-aware: only scan entities whose low 16 bits match partition_id
      if (partition_id_ != 0 && (id & 0xFFFF) != partition_id_) {
        continue;
      }
      std::vector<std::pair<Timestamp, Descriptor>> versions;
      auto s = storage_client_->ScanNode(id, Timestamp::Max(), &versions);
      if (s.ok() && !versions.empty()) {
        results.push_back(id);
      }
    }
    return results;
  };

  // Wire up outgoing edge expansion
  this->get_out_neighbors_fn = [this](uint64_t vertex_id, uint16_t edge_type,
                                       Timestamp start, Timestamp end) {
    std::vector<Neighbor> results;
    if (!storage_client_) return results;
    std::vector<EdgeScanEntry> edges;
    auto s = storage_client_->ScanOutEdges(vertex_id, edge_type, end, &edges);
    if (!s.ok()) return results;
    for (const auto& e : edges) {
      results.emplace_back(e.target_id, e.edge_type, e.timestamp, std::nullopt);
    }
    return results;
  };

  // Wire up incoming edge expansion
  this->get_in_neighbors_fn = [this](uint64_t vertex_id, uint16_t edge_type,
                                      Timestamp start, Timestamp end) {
    std::vector<Neighbor> results;
    if (!storage_client_) return results;
    std::vector<EdgeScanEntry> edges;
    auto s = storage_client_->ScanInEdges(vertex_id, edge_type, end, &edges);
    if (!s.ok()) return results;
    for (const auto& e : edges) {
      results.emplace_back(e.target_id, e.edge_type, e.timestamp, std::nullopt);
    }
    return results;
  };
}

}  // namespace queryd
}  // namespace cedar
