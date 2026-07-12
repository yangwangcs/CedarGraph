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
// QueryDispatcher - Routes traversal requests to local compute or bootstrap
// =============================================================================

#pragma once

#include <grpcpp/grpcpp.h>
#include <mutex>
#include <set>
#include <unordered_map>

#include "cedar/gcn/tmv_engine.h"
#include "gcn_service.pb.h"

namespace cedar {
namespace gcn {

class StorageBackfillService;

class QueryDispatcher {
 public:
  explicit QueryDispatcher(TMVEngine* engine);

  void SetBackfillService(StorageBackfillService* svc) {
    backfill_service_ = svc;
  }

  void SetPartitionProgress(uint32_t partition_id,
                            uint64_t partition_epoch,
                            uint64_t applied_version,
                            bool query_ready);
  uint64_t MinimumActiveQueryVersion() const;

  // Routes a traversal request:
  //   - Local hit: vertex cached with edges at query_time -> fill response
  //   - Local miss: vertex not cached -> empty response (bootstrap in Task 4.x)
  grpc::Status DispatchTraversal(const TraversalRequest& req,
                                 TraversalResponse* resp);

  // Routes a distributed sub-query request:
  //   - Local hit: fills next_entity_ids from TMVEngine
  //   - Local miss: attempts lazy backfill if backfill_service_ is available
  grpc::Status DispatchSubQuery(const SubQueryRequest& req,
                                SubQueryResponse* resp);

  TMVEngine* engine() const { return engine_; }

 private:
  struct PartitionProgress {
    uint64_t partition_epoch = 0;
    uint64_t applied_version = 0;
    bool query_ready = false;
  };

  class ActiveQueryRegistration {
   public:
    ActiveQueryRegistration(QueryDispatcher* dispatcher, uint64_t version);
    ~ActiveQueryRegistration();

    ActiveQueryRegistration(const ActiveQueryRegistration&) = delete;
    ActiveQueryRegistration& operator=(const ActiveQueryRegistration&) = delete;

   private:
    QueryDispatcher* dispatcher_;
    uint64_t version_;
    bool registered_;
  };

  void RegisterActiveQuery(uint64_t version);
  void UnregisterActiveQuery(uint64_t version);
  static uint64_t ActiveQueryVersion(const TraversalRequest& req);

  TMVEngine* engine_;
  class StorageBackfillService* backfill_service_ = nullptr;
  mutable std::mutex progress_mutex_;
  std::unordered_map<uint32_t, PartitionProgress> partition_progress_;
  mutable std::mutex active_query_mutex_;
  std::multiset<uint64_t> active_query_versions_;
};

}  // namespace gcn
}  // namespace cedar
