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

#include "cedar/gcn/gcn_service.h"


namespace cedar {
namespace gcn {

GcnServiceImpl::GcnServiceImpl(
    std::function<void(const cedar::gcn::CDCEvent&)> on_event_callback)
    : on_event_callback_(std::move(on_event_callback)) {}

GcnServiceImpl::GcnServiceImpl(
    TMVEngine* engine,
    std::function<void(const cedar::gcn::CDCEvent&)> on_event_callback)
    : on_event_callback_(std::move(on_event_callback)),
      dispatcher_(std::make_unique<QueryDispatcher>(engine)) {}

GcnServiceImpl::GcnServiceImpl(
    TMVEngine* engine,
    StorageBackfillService* backfill_service,
    std::function<void(const cedar::gcn::CDCEvent&)> on_event_callback)
    : on_event_callback_(std::move(on_event_callback)),
      dispatcher_(std::make_unique<QueryDispatcher>(engine)) {
  if (dispatcher_ && backfill_service) {
    dispatcher_->SetBackfillService(backfill_service);
  }
}

void GcnServiceImpl::Shutdown() {
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    stream_closed_ = true;
  }
  queue_cv_.notify_all();
}

GcnServiceImpl::~GcnServiceImpl() {
  Shutdown();
}

void GcnServiceImpl::EnqueueEvent(const CDCEvent& event) {
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    pending_events_.push(event);
  }
  queue_cv_.notify_one();
}

grpc::Status GcnServiceImpl::Traverse(grpc::ServerContext* context,
                                      const TraversalRequest* request,
                                      TraversalResponse* response) {
  if (context->IsCancelled()) return grpc::Status::CANCELLED;
  if (dispatcher_) {
    auto status = dispatcher_->DispatchTraversal(*request, response);
    // On local miss, try distributed routing via ScatterGatherRouter
    if (!response->success() && router_) {
      *response = router_->ScatterTraversalByEntity(*request);
    }
    return status;
  }
  response->Clear();
  response->set_success(false);
  response->set_error_msg("Dispatcher not available");
  return grpc::Status::OK;
}

grpc::Status GcnServiceImpl::SubQuery(grpc::ServerContext* context,
                                      const SubQueryRequest* request,
                                      SubQueryResponse* response) {
  if (context->IsCancelled()) return grpc::Status::CANCELLED;
  response->Clear();
  response->set_trace_id(request->trace_id());

  if (!dispatcher_) {
    response->set_success(false);
    response->set_error_msg("Dispatcher not available");
    return grpc::Status::OK;
  }

  // Delegate to QueryDispatcher which uses TMVEngine for local traversal
  auto status = dispatcher_->DispatchSubQuery(*request, response);

  // On local miss, try distributed routing via consistent hash
  if (!response->success() && router_) {
    *response = router_->ScatterByEntity(*request);
  }

  return status;
}

grpc::Status GcnServiceImpl::OnCacheInvalidate(grpc::ServerContext* context,
                                               const CacheInvalidateNotice* request,
                                               Empty* response) {
  if (context->IsCancelled()) return grpc::Status::CANCELLED;
  response->Clear();

  if (dispatcher_ && dispatcher_->engine()) {
    size_t freed = dispatcher_->engine()->InvalidateVertex(request->entity_id());
    (void)freed;
  }

  return grpc::Status::OK;
}

grpc::Status GcnServiceImpl::OnEventStream(
    grpc::ServerContext* context,
    grpc::ServerReaderWriter<CDCEvent, Ack>* stream) {
  while (!context->IsCancelled()) {
    CDCEvent event;
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_cv_.wait(
          lock,
          [this] { return !pending_events_.empty() || stream_closed_; });
      if (pending_events_.empty()) {
        break;
      }
      event = pending_events_.front();
      pending_events_.pop();
    }

    if (on_event_callback_) {
      on_event_callback_(event);
    }

    if (!stream->Write(event)) {
      break;
    }

    Ack ack;
    if (!stream->Read(&ack)) {
      break;
    }
  }

  return grpc::Status::OK;
}

}  // namespace gcn
}  // namespace cedar
