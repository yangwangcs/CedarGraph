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

// =============================================================================
// QueryDispatcher Implementation
// =============================================================================

#include "cedar/gcn/query_dispatcher.h"

#include "cedar/gcn/storage_backfill_service.h"

#include <limits>

namespace cedar {
namespace gcn {

QueryDispatcher::QueryDispatcher(TMVEngine* engine)
    : engine_(engine), backfill_service_(nullptr) {}

void QueryDispatcher::SetPartitionProgress(uint32_t partition_id,
                                           uint64_t partition_epoch,
                                           uint64_t applied_version,
                                           bool query_ready) {
  std::lock_guard<std::mutex> lock(progress_mutex_);
  partition_progress_[partition_id] =
      PartitionProgress{partition_epoch, applied_version, query_ready};
}

QueryDispatcher::ActiveQueryRegistration::ActiveQueryRegistration(
    QueryDispatcher* dispatcher, uint64_t version)
    : dispatcher_(dispatcher),
      version_(version),
      registered_(dispatcher != nullptr && version != 0) {
  if (registered_) {
    dispatcher_->RegisterActiveQuery(version_);
  }
}

QueryDispatcher::ActiveQueryRegistration::~ActiveQueryRegistration() {
  if (registered_) {
    dispatcher_->UnregisterActiveQuery(version_);
  }
}

void QueryDispatcher::RegisterActiveQuery(uint64_t version) {
  std::lock_guard<std::mutex> lock(active_query_mutex_);
  active_query_versions_.insert(version);
}

void QueryDispatcher::UnregisterActiveQuery(uint64_t version) {
  std::lock_guard<std::mutex> lock(active_query_mutex_);
  auto it = active_query_versions_.find(version);
  if (it != active_query_versions_.end()) {
    active_query_versions_.erase(it);
  }
}

uint64_t QueryDispatcher::MinimumActiveQueryVersion() const {
  std::lock_guard<std::mutex> lock(active_query_mutex_);
  if (active_query_versions_.empty()) {
    return std::numeric_limits<uint64_t>::max();
  }
  return *active_query_versions_.begin();
}

uint64_t QueryDispatcher::ActiveQueryVersion(const TraversalRequest& req) {
  return req.query_time();
}

grpc::Status QueryDispatcher::DispatchTraversal(const TraversalRequest& req,
                                                TraversalResponse* resp) {
  ActiveQueryRegistration active_query(this, ActiveQueryVersion(req));
  resp->Clear();
  resp->set_trace_id(req.trace_id());

  uint64_t served_version = 0;
  if (req.has_partition_id()) {
    std::lock_guard<std::mutex> lock(progress_mutex_);
    auto it = partition_progress_.find(req.partition_id());
    if (it == partition_progress_.end() || !it->second.query_ready) {
      resp->set_success(false);
      resp->set_cache_status(CACHE_STATUS_NOT_READY);
      return grpc::Status::OK;
    }
    resp->set_partition_epoch(it->second.partition_epoch);
    served_version = it->second.applied_version;
    resp->set_served_version(served_version);
    if (req.required_version() > served_version) {
      resp->set_success(false);
      resp->set_cache_status(CACHE_STATUS_VERSION_LAG);
      return grpc::Status::OK;
    }
  }

  if (!engine_) {
    resp->set_success(false);
    resp->set_error_msg("TMVEngine not available");
    resp->set_cache_status(CACHE_STATUS_MISS);
    return grpc::Status::OK;
  }

  // Scan outbound edges at the requested query time.
  // TraversalRequest does not carry a direction field; outbound is the
  // default traversal direction.
  std::vector<TMVEdge> edges =
      engine_->ScanAtTime(req.root_entity_id(), Direction::kOut,
                          req.query_time());

  if (!edges.empty()) {
    // Local hit: populate response with reachable entity ids
    resp->set_success(true);
    resp->set_cache_status(CACHE_STATUS_HIT);
    resp->set_served_version(served_version);
    for (const auto& edge : edges) {
      resp->add_visited_entity_ids(edge.target_id);
    }
  } else {
    // Local miss: return empty response.
    // Bootstrap-then-retry logic will be added in Task 4.x.
    resp->set_success(false);
    resp->set_cache_status(CACHE_STATUS_MISS);
    resp->set_served_version(served_version);
  }

  return grpc::Status::OK;
}

grpc::Status QueryDispatcher::DispatchSubQuery(const SubQueryRequest& req,
                                               SubQueryResponse* resp) {
  ActiveQueryRegistration active_query(this, req.query_time());
  resp->Clear();
  resp->set_trace_id(req.trace_id());

  if (!engine_) {
    resp->set_success(false);
    resp->set_error_msg("TMVEngine not available");
    return grpc::Status::OK;
  }

  std::vector<TMVEdge> edges =
      engine_->ScanAtTime(req.current_entity_id(), Direction::kOut,
                          req.query_time());

  if (edges.empty() && backfill_service_) {
    // Lazy backfill: fetch from StorageD on cache miss
    backfill_service_->BackfillVertex(req.current_entity_id(), /*edge_type=*/0);
    edges = engine_->ScanAtTime(req.current_entity_id(), Direction::kOut,
                                req.query_time());
  }

  if (!edges.empty()) {
    resp->set_success(true);
    for (const auto& edge : edges) {
      resp->add_next_entity_ids(edge.target_id);
    }
  } else {
    resp->set_success(false);
    resp->set_error_msg("No edges found for entity " +
                        std::to_string(req.current_entity_id()));
  }

  return grpc::Status::OK;
}

}  // namespace gcn
}  // namespace cedar
