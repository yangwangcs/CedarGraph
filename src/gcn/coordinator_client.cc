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
#include <string>

#include "meta_service.grpc.pb.h"

namespace cedar {
namespace gcn {
namespace {

uint64_t NowMs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

cedar::Status StatusFromGrpcStatus(const grpc::Status& status,
                                   const std::string& op) {
  if (status.ok()) return cedar::Status::OK();
  switch (status.error_code()) {
    case grpc::StatusCode::NOT_FOUND:
      return cedar::Status::NotFound(op, status.error_message());
    case grpc::StatusCode::ABORTED:
      return cedar::Status::Conflict(op, status.error_message());
    case grpc::StatusCode::INVALID_ARGUMENT:
      return cedar::Status::InvalidArgument(op, status.error_message());
    case grpc::StatusCode::FAILED_PRECONDITION:
      return cedar::Status::NotLeader(op, status.error_message());
    case grpc::StatusCode::UNAVAILABLE:
      return cedar::Status::Unavailable(op, status.error_message());
    case grpc::StatusCode::CANCELLED:
      return cedar::Status::Cancelled(op, status.error_message());
    default:
      return cedar::Status::IOError(op, status.error_message());
  }
}

cedar::Status StatusFromGcnLeaseResponse(
    cedar::meta::GcnLeaseStatusCode code,
    const std::string& message) {
  switch (code) {
    case cedar::meta::GCN_LEASE_STATUS_OK:
      return cedar::Status::OK();
    case cedar::meta::GCN_LEASE_STATUS_STALE_INCARNATION:
    case cedar::meta::GCN_LEASE_STATUS_STALE_LEASE:
      return cedar::Status::Conflict(message);
    case cedar::meta::GCN_LEASE_STATUS_NO_ELIGIBLE_GCN:
      return cedar::Status::NotFound(message);
    case cedar::meta::GCN_LEASE_STATUS_NOT_LEADER:
      return cedar::Status::NotLeader(message);
    case cedar::meta::GCN_LEASE_STATUS_INVALID_ARGUMENT:
      return cedar::Status::InvalidArgument(message);
    default:
      return cedar::Status::IOError(message);
  }
}

}  // namespace

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

cedar::Status CoordinatorClient::RegisterGcn(uint64_t gcn_id,
                                             const std::string& endpoint,
                                             uint64_t incarnation) {
  if (!stub_) return cedar::Status::InvalidArgument("No stub");

  cedar::meta::RegisterGcnRequest req;
  req.set_gcn_id(gcn_id);
  req.set_endpoint(endpoint);
  req.set_incarnation(incarnation);
  req.set_last_heartbeat_ms(NowMs());

  auto call = [&](cedar::meta::MetaService::Stub* stub,
                  cedar::meta::RegisterGcnResponse* resp) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    return stub->RegisterGcn(&ctx, req, resp);
  };

  cedar::meta::RegisterGcnResponse resp;
  grpc::Status status = call(stub_.get(), &resp);
  if (!status.ok()) {
    return StatusFromGrpcStatus(status, "RegisterGcn RPC failed");
  }
  if (!resp.success() &&
      resp.status_code() == cedar::meta::GCN_LEASE_STATUS_NOT_LEADER &&
      !resp.leader_address().empty()) {
    auto leader_stub = cedar::meta::MetaService::NewStub(grpc::CreateChannel(
        resp.leader_address(), grpc::InsecureChannelCredentials()));
    cedar::meta::RegisterGcnResponse retry_resp;
    status = call(leader_stub.get(), &retry_resp);
    if (!status.ok()) {
      return StatusFromGrpcStatus(status, "RegisterGcn RPC failed");
    }
    resp = retry_resp;
  }
  if (!resp.success()) {
    return StatusFromGcnLeaseResponse(resp.status_code(), resp.error_msg());
  }
  return cedar::Status::OK();
}

cedar::StatusOr<std::vector<cedar::dtx::GcnLease>>
CoordinatorClient::RenewGcnLeases(
    uint64_t gcn_id,
    uint64_t incarnation,
    const std::vector<cedar::dtx::GcnPartitionProgress>& progress) {
  if (!stub_) return cedar::Status::InvalidArgument("No stub");

  cedar::meta::RenewGcnLeasesRequest req;
  req.set_gcn_id(gcn_id);
  req.set_incarnation(incarnation);
  for (const auto& item : progress) {
    auto* proto = req.add_progress();
    proto->set_partition_id(item.partition_id);
    proto->set_partition_epoch(item.partition_epoch);
    proto->set_applied_offset(item.applied_offset);
    proto->set_applied_version(item.applied_version);
    proto->set_query_ready(item.query_ready);
  }

  auto call = [&](cedar::meta::MetaService::Stub* stub,
                  cedar::meta::RenewGcnLeasesResponse* resp) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    return stub->RenewGcnLeases(&ctx, req, resp);
  };

  cedar::meta::RenewGcnLeasesResponse resp;
  grpc::Status status = call(stub_.get(), &resp);
  if (!status.ok()) {
    return StatusFromGrpcStatus(status, "RenewGcnLeases RPC failed");
  }
  if (!resp.success() &&
      resp.status_code() == cedar::meta::GCN_LEASE_STATUS_NOT_LEADER &&
      !resp.leader_address().empty()) {
    auto leader_stub = cedar::meta::MetaService::NewStub(grpc::CreateChannel(
        resp.leader_address(), grpc::InsecureChannelCredentials()));
    cedar::meta::RenewGcnLeasesResponse retry_resp;
    status = call(leader_stub.get(), &retry_resp);
    if (!status.ok()) {
      return StatusFromGrpcStatus(status, "RenewGcnLeases RPC failed");
    }
    resp = retry_resp;
  }
  if (!resp.success()) {
    return StatusFromGcnLeaseResponse(resp.status_code(), resp.error_msg());
  }
  std::vector<cedar::dtx::GcnLease> leases;
  leases.reserve(resp.leases_size());
  for (const auto& proto : resp.leases()) {
    leases.push_back(cedar::dtx::GcnLease{
        proto.partition_id(),
        proto.gcn_id(),
        proto.lease_epoch(),
        proto.expires_at_ms(),
        proto.lease_token(),
    });
  }
  return leases;
}

cedar::StatusOr<cedar::dtx::GcnRoute> CoordinatorClient::LocateGcn(
    uint32_t partition_id,
    uint64_t required_version) {
  if (!stub_) return cedar::Status::InvalidArgument("No stub");

  cedar::meta::LocateGcnRequest req;
  req.set_partition_id(partition_id);
  req.set_required_version(required_version);

  cedar::meta::LocateGcnResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

  grpc::Status status = stub_->LocateGcn(&ctx, req, &resp);
  if (!status.ok()) {
    return StatusFromGrpcStatus(status, "LocateGcn RPC failed");
  }
  if (!resp.success()) {
    return StatusFromGcnLeaseResponse(resp.status_code(), resp.error_msg());
  }
  const auto& proto = resp.route();
  return cedar::dtx::GcnRoute{
      proto.partition_id(),
      proto.gcn_id(),
      proto.endpoint(),
      proto.lease_epoch(),
      proto.applied_version(),
      proto.expires_at_ms(),
  };
}

}  // namespace gcn
}  // namespace cedar
