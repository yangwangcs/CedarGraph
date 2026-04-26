// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cedar/gcn/storage_backfill_service.h"

#include <limits>

#include "cedar/gcn/tmv_engine.h"
#include "cedar/gcn/tmv_edge.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/edge_scan_entry.h"

namespace cedar {
namespace gcn {

StorageBackfillService::StorageBackfillService(TMVEngine* tmv_engine,
                                               CedarGraphStorage* storage)
    : tmv_engine_(tmv_engine), storage_(storage) {}

void StorageBackfillService::BackfillVertex(uint64_t entity_id,
                                            uint16_t edge_type) {
  if (!tmv_engine_ || !storage_) {
    return;
  }

  auto edges = storage_->ScanEdgesWithFolding(
      entity_id, EntityType::EdgeOut, edge_type, Timestamp::Max());

  std::vector<TMVEdge> tmv_edges;
  tmv_edges.reserve(edges.size());
  for (const auto& e : edges) {
    TMVEdge edge{};
    edge.target_id = e.target_id;
    edge.valid_from = static_cast<uint32_t>(e.timestamp.value());
    edge.valid_to = std::numeric_limits<uint32_t>::max();
    edge.attr_offset = 0;
    edge.edge_type = e.edge_type;
    edge.reserved = 0;
    tmv_edges.push_back(edge);
  }

  tmv_engine_->BootstrapVertex(entity_id, Direction::kOut, tmv_edges, false);
}

void StorageBackfillService::BackfillRange(uint64_t start_id,
                                           uint64_t end_id,
                                           uint16_t edge_type) {
  if (!tmv_engine_ || !storage_) {
    return;
  }

  for (uint64_t id = start_id; id <= end_id; ++id) {
    BackfillVertex(id, edge_type);
  }
}

}  // namespace gcn
}  // namespace cedar
