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
// CoordinatorClient Implementation
// =============================================================================

#include "cedar/gcn/coordinator_client.h"

#include <chrono>

#include "meta_service.grpc.pb.h"

namespace cedar {
namespace gcn {

CoordinatorClient::CoordinatorClient(std::shared_ptr<grpc::Channel> channel)
    : channel_(std::move(channel)) {
  if (channel_) {
    stub_ = cedar::meta::MetaService::NewStub(channel_);
  }
}

std::optional<coordinator::CacheWindow> CoordinatorClient::Locate(
    uint64_t entity_id, uint64_t query_time) {
  if (!stub_) return std::nullopt;

  cedar::meta::LocateCacheRequest req;
  req.set_entity_id(entity_id);
  req.set_query_time(query_time);

  cedar::meta::LocateCacheResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

  grpc::Status status = stub_->LocateCache(&ctx, req, &resp);
  if (!status.ok() || !resp.found()) {
    return std::nullopt;
  }

  coordinator::CacheWindow window;
  window.entity_id = resp.window().entity_id();
  window.cached_from = resp.window().cached_from();
  window.cached_to = resp.window().cached_to();
  window.gcn_node_id = resp.window().gcn_node_id();
  window.version = resp.window().version();
  window.expire_at = resp.window().expire_at();
  return window;
}

cedar::Status CoordinatorClient::ReportCache(const coordinator::CacheWindow& window) {
  if (!stub_) return cedar::Status::InvalidArgument("No stub");

  cedar::meta::ReportCacheRequest req;
  auto* w = req.mutable_window();
  w->set_entity_id(window.entity_id);
  w->set_cached_from(window.cached_from);
  w->set_cached_to(window.cached_to);
  w->set_gcn_node_id(window.gcn_node_id);
  w->set_version(window.version);
  w->set_expire_at(window.expire_at);

  cedar::meta::ReportCacheResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

  grpc::Status status = stub_->ReportCache(&ctx, req, &resp);
  if (!status.ok()) {
    return cedar::Status::IOError("ReportCache RPC failed: " + status.error_message());
  }
  return cedar::Status::OK();
}

cedar::Status CoordinatorClient::Heartbeat(
    const std::vector<coordinator::CacheWindow>& windows) {
  if (!stub_) return cedar::Status::InvalidArgument("No stub");

  cedar::meta::GcnHeartbeatRequest req;
  req.set_gcn_node_id(gcn_node_id_);
  for (const auto& w : windows) {
    auto* cw = req.add_windows();
    cw->set_entity_id(w.entity_id);
    cw->set_cached_from(w.cached_from);
    cw->set_cached_to(w.cached_to);
    cw->set_gcn_node_id(w.gcn_node_id);
    cw->set_version(w.version);
    cw->set_expire_at(w.expire_at);
  }

  cedar::meta::GcnHeartbeatResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

  grpc::Status status = stub_->GcnHeartbeat(&ctx, req, &resp);
  if (!status.ok()) {
    return cedar::Status::IOError("GcnHeartbeat RPC failed: " + status.error_message());
  }
  return cedar::Status::OK();
}

}  // namespace gcn
}  // namespace cedar
