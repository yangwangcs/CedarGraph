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
// GcnService Implementation - Graph Compute Node RPC Service Stub
// =============================================================================

#ifndef CEDAR_GCN_GCN_SERVICE_H_
#define CEDAR_GCN_GCN_SERVICE_H_

#include <condition_variable>
#include <functional>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <queue>

#include "cedar/gcn/query_dispatcher.h"
#include "cedar/gcn/scatter_gather_router.h"
#include "gcn_service.grpc.pb.h"

namespace cedar {
namespace gcn {

class GcnServiceImpl final : public GcnService::Service {
 public:
  explicit GcnServiceImpl(
      std::function<void(const cedar::gcn::CDCEvent&)> on_event_callback = nullptr);
  explicit GcnServiceImpl(
      TMVEngine* engine,
      std::function<void(const cedar::gcn::CDCEvent&)> on_event_callback = nullptr);
  explicit GcnServiceImpl(
      TMVEngine* engine,
      StorageBackfillService* backfill_service,
      std::function<void(const cedar::gcn::CDCEvent&)> on_event_callback = nullptr);
  ~GcnServiceImpl() override;

  // Inject a ScatterGatherRouter for distributed sub-query / traversal routing.
  void SetScatterGatherRouter(std::shared_ptr<ScatterGatherRouter> router) {
    router_ = std::move(router);
  }

  // Disable copy
  GcnServiceImpl(const GcnServiceImpl&) = delete;
  GcnServiceImpl& operator=(const GcnServiceImpl&) = delete;

  // Enqueue a CDC event to be streamed to connected clients.
  void EnqueueEvent(const CDCEvent& event);

  // Graph traversal from a root entity
  grpc::Status Traverse(grpc::ServerContext* context,
                        const TraversalRequest* request,
                        TraversalResponse* response) override;

  // Distributed sub-query across GCNs
  grpc::Status SubQuery(grpc::ServerContext* context,
                        const SubQueryRequest* request,
                        SubQueryResponse* response) override;

  // Cache invalidation notice
  grpc::Status OnCacheInvalidate(grpc::ServerContext* context,
                                 const CacheInvalidateNotice* request,
                                 Empty* response) override;

  // Event stream for change data capture
  grpc::Status OnEventStream(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<CDCEvent, Ack>* stream) override;

 private:
  std::function<void(const cedar::gcn::CDCEvent&)> on_event_callback_;
  std::unique_ptr<QueryDispatcher> dispatcher_;
  std::shared_ptr<ScatterGatherRouter> router_;
  std::queue<CDCEvent> pending_events_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  bool stream_closed_ = false;
};

}  // namespace gcn
}  // namespace cedar

#endif  // CEDAR_GCN_GCN_SERVICE_H_
