#include "cedar/queryd/storage_execution_context.h"
#include "cedar/types/edge_scan_entry.h"

namespace cedar {
namespace queryd {

StorageBackedExecutionContext::StorageBackedExecutionContext(
    QueryStorageClient* storage_client, uint32_t partition_id,
    const std::string& space_name, const std::string& label)
    : storage_client_(storage_client),
      partition_id_(partition_id),
      space_name_(space_name),
      label_(label) {
  this->partition_id = partition_id;

  // Label-index-based enumeration (fast path)
  if (!label_.empty()) {
    this->get_all_entities_fn = [this](uint64_t min_id, uint64_t max_id,
                                       uint64_t step) {
      std::vector<uint64_t> results;
      if (!storage_client_) return results;

      Status s = storage_client_->ScanLabel(space_name_, label_, min_id,
                                            max_id, 50, &results);
      if (!s.ok()) {
        // Fall back to sequential scan on error
        SequentialEntityScan(min_id, max_id, step, &results);
      }
      return results;
    };
  } else {
    // Sequential entity scan (slow path — no label)
    this->get_all_entities_fn = [this](uint64_t min_id, uint64_t max_id,
                                       uint64_t step) {
      std::vector<uint64_t> results;
      if (!storage_client_) return results;
      SequentialEntityScan(min_id, max_id, step, &results);
      return results;
    };
  }

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

void StorageBackedExecutionContext::SequentialEntityScan(
    uint64_t min_id, uint64_t max_id, uint64_t step,
    std::vector<uint64_t>* results) {
  constexpr size_t kMaxScanResults = 50;
  constexpr uint64_t kMaxConsecutiveMisses = 10;
  uint64_t effective_max = std::min(max_id, min_id + 100);
  uint64_t consecutive_misses = 0;

  for (uint64_t id = min_id;
       id <= effective_max && results->size() < kMaxScanResults; id += step) {
    if (partition_id_ != 0 && (id & 0xFFFF) != partition_id_) continue;

    std::vector<std::pair<Timestamp, Descriptor>> versions;
    auto s = storage_client_->ScanNode(id, Timestamp::Max(), &versions);
    if (s.ok() && !versions.empty()) {
      results->push_back(id);
      consecutive_misses = 0;
    } else {
      ++consecutive_misses;
      if (consecutive_misses >= kMaxConsecutiveMisses && !results->empty()) {
        break;
      }
    }
  }
}

}  // namespace queryd
}  // namespace cedar
