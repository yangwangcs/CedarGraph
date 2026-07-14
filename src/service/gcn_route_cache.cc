// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "cedar/service/gcn_route_cache.h"

#include <chrono>

namespace cedar {
namespace service {

namespace {

uint64_t NowMillis() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

}  // namespace

GcnRouteCache::GcnRouteCache(LocateFn locate_fn, ChannelFactory channel_factory)
    : locate_fn_(std::move(locate_fn)),
      channel_factory_(std::move(channel_factory)) {}

StatusOr<cedar::gcn::TraversalResponse> GcnRouteCache::Traverse(
    uint32_t partition_id,
    uint64_t required_version,
    const cedar::gcn::TraversalRequest& request) {
  if (!locate_fn_) {
    return Status::Unavailable("GCN locator is not configured");
  }

  auto route_result = locate_fn_(partition_id, required_version);
  if (!route_result.ok()) {
    return route_result.status();
  }
  const auto route = route_result.ValueOrDie();
  if (route.partition_id != partition_id) {
    return Status::Unavailable("GCN route partition mismatch");
  }
  if (route.endpoint.empty()) {
    return Status::Unavailable("GCN route endpoint is empty");
  }
  if (required_version > 0 && route.applied_version < required_version) {
    return Status::Unavailable("GCN route is behind required version");
  }
  if (route.expires_at_ms > 0 && route.expires_at_ms < NowMillis()) {
    return Status::Unavailable("GCN route lease expired");
  }

  auto stub = GetStub(route.endpoint);
  if (!stub) {
    return Status::Unavailable("GCN stub is unavailable");
  }

  cedar::gcn::TraversalResponse response;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto grpc_status = stub->Traverse(&ctx, request, &response);
  if (!grpc_status.ok()) {
    return Status::Unavailable("GCN Traverse RPC failed: " + grpc_status.error_message());
  }

  std::string reason;
  if (!IsAcceptableTraversalResponse(route, required_version, response, &reason)) {
    return Status::Unavailable(reason);
  }
  return response;
}

bool GcnRouteCache::IsAcceptableTraversalResponse(
    const cedar::dtx::GcnRoute& route,
    uint64_t required_version,
    const cedar::gcn::TraversalResponse& response,
    std::string* reason) {
  if (!response.success()) {
    if (reason) *reason = "GCN response is not successful: " + response.error_msg();
    return false;
  }
  if (response.cache_status() == cedar::gcn::CACHE_STATUS_VERSION_LAG) {
    if (reason) *reason = "GCN cache is version-lagged";
    return false;
  }
  if (response.cache_status() == cedar::gcn::CACHE_STATUS_NOT_READY) {
    if (reason) *reason = "GCN cache is not ready";
    return false;
  }
  if (required_version > 0 && response.served_version() < required_version) {
    if (reason) *reason = "GCN served version is behind required version";
    return false;
  }
  // MetaD's GcnRoute::lease_epoch identifies the GCN lease, not the StorageD
  // partition epoch. Until StorageD exposes an epoch in LocateGcn, GraphD cannot
  // compare equality here. The conservative check is to require the GCN to
  // return a non-zero partition_epoch and never substitute route.lease_epoch.
  if (response.partition_epoch() == 0) {
    if (reason) *reason = "GCN response lacks a verifiable partition epoch";
    return false;
  }
  (void)route;
  return true;
}

std::shared_ptr<cedar::gcn::GcnService::Stub> GcnRouteCache::GetStub(
    const std::string& endpoint) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = stubs_.find(endpoint);
  if (it != stubs_.end()) {
    return it->second;
  }
  if (!channel_factory_) {
    return nullptr;
  }
  auto channel = channel_factory_(endpoint);
  if (!channel) {
    return nullptr;
  }
  auto stub = cedar::gcn::GcnService::NewStub(channel);
  auto shared_stub = std::shared_ptr<cedar::gcn::GcnService::Stub>(std::move(stub));
  stubs_[endpoint] = shared_stub;
  return shared_stub;
}

}  // namespace service
}  // namespace cedar
