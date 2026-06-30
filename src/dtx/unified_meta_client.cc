// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Unified Meta Client Implementation

#include "cedar/dtx/unified_meta_client.h"

#include <chrono>
#include <thread>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "cedar/core/logging.h"
#include "cedar/dtx/raft/grpc_tls.h"
#include "meta_service.pb.h"
#include "meta_service.grpc.pb.h"

namespace cedar {
namespace dtx {

using namespace std::chrono;

// ============================================================================
// UnifiedMetaClient
// ============================================================================

UnifiedMetaClient::UnifiedMetaClient(const Options& options)
    : options_(options) {}

UnifiedMetaClient::~UnifiedMetaClient() {
  Shutdown();
}

Status UnifiedMetaClient::Init() {
  auto creds =
      cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnvStrict();
  if (!creds.ok()) {
    return Status::IOError(
        "Failed to create TLS credentials for MetaD: " +
        creds.status().ToString());
  }
  channel_ = grpc::CreateChannel(options_.meta_service_address,
                                 creds.ValueOrDie());

  Status s = FetchPartitionMapFromMeta();
  if (!s.ok()) {
    return s;
  }

  running_ = true;
  refresh_thread_ = std::thread(&UnifiedMetaClient::RefreshLoop, this);

  if (options_.enable_watch) {
    watch_thread_ = std::thread(&UnifiedMetaClient::WatchLoop, this);
  }

  return Status::OK();
}

Status UnifiedMetaClient::Shutdown() {
  running_ = false;
  shutdown_cv_.notify_all();
  {
    std::lock_guard<std::mutex> lock(watch_context_mutex_);
    if (active_watch_context_) {
      active_watch_context_->TryCancel();
    }
  }
  if (refresh_thread_.joinable()) {
    refresh_thread_.join();
  }
  if (watch_thread_.joinable()) {
    watch_thread_.join();
  }
  return Status::OK();
}

Status UnifiedMetaClient::GetLeaderAddress(PartitionID partition_id,
                                           std::string* address) {
  std::shared_lock<std::shared_mutex> lock(state_mutex_);
  auto it = partition_map_.find(partition_id);
  if (it != partition_map_.end()) {
    if (!it->second.IsExpired(options_.route_cache_ttl_sec)) {
      *address = it->second.leader_address;
      return Status::OK();
    }
  }
  lock.unlock();

  Status s = RefreshPartitionMap();
  if (!s.ok()) return s;

  std::shared_lock<std::shared_mutex> lock2(state_mutex_);
  it = partition_map_.find(partition_id);
  if (it != partition_map_.end()) {
    *address = it->second.leader_address;
    return Status::OK();
  }
  return Status::NotFound("Partition not found: " +
                          std::to_string(partition_id));
}

Status UnifiedMetaClient::GetLeaderNode(PartitionID partition_id,
                                        NodeID* node_id) {
  std::shared_lock<std::shared_mutex> lock(state_mutex_);
  auto it = partition_map_.find(partition_id);
  if (it != partition_map_.end()) {
    if (!it->second.IsExpired(options_.route_cache_ttl_sec)) {
      *node_id = it->second.leader_node;
      return Status::OK();
    }
  }
  lock.unlock();

  Status s = RefreshPartitionMap();
  if (!s.ok()) return s;

  std::shared_lock<std::shared_mutex> lock2(state_mutex_);
  it = partition_map_.find(partition_id);
  if (it != partition_map_.end()) {
    *node_id = it->second.leader_node;
    return Status::OK();
  }
  return Status::NotFound("Partition not found: " +
                          std::to_string(partition_id));
}

Status UnifiedMetaClient::GetFollowerAddresses(
    PartitionID partition_id, std::vector<std::string>* addresses) {
  std::shared_lock<std::shared_mutex> lock(state_mutex_);
  auto it = partition_map_.find(partition_id);
  if (it != partition_map_.end()) {
    if (!it->second.IsExpired(options_.route_cache_ttl_sec)) {
      *addresses = it->second.follower_addresses;
      return Status::OK();
    }
  }
  lock.unlock();

  Status s = RefreshPartitionMap();
  if (!s.ok()) return s;

  std::shared_lock<std::shared_mutex> lock2(state_mutex_);
  it = partition_map_.find(partition_id);
  if (it != partition_map_.end()) {
    *addresses = it->second.follower_addresses;
    return Status::OK();
  }
  return Status::NotFound("Partition not found: " +
                          std::to_string(partition_id));
}

Status UnifiedMetaClient::GetPartitionRoute(PartitionID partition_id,
                                            UnifiedPartitionInfo* info) {
  std::shared_lock<std::shared_mutex> lock(state_mutex_);
  auto it = partition_map_.find(partition_id);
  if (it != partition_map_.end()) {
    if (!it->second.IsExpired(options_.route_cache_ttl_sec)) {
      *info = it->second;
      return Status::OK();
    }
  }
  lock.unlock();

  Status s = RefreshPartitionMap();
  if (!s.ok()) return s;

  std::shared_lock<std::shared_mutex> lock2(state_mutex_);
  it = partition_map_.find(partition_id);
  if (it != partition_map_.end()) {
    *info = it->second;
    return Status::OK();
  }
  return Status::NotFound("Partition not found: " +
                          std::to_string(partition_id));
}

void UnifiedMetaClient::InvalidatePartition(PartitionID partition_id) {
  std::unique_lock<std::shared_mutex> lock(state_mutex_);
  partition_map_.erase(partition_id);
}

void UnifiedMetaClient::InvalidateAll() {
  std::unique_lock<std::shared_mutex> lock(state_mutex_);
  partition_map_.clear();
  cluster_version_ = 0;
}

Status UnifiedMetaClient::RefreshPartitionMap() {
  return FetchPartitionMapFromMeta();
}

void UnifiedMetaClient::WatchPartitionMap(
    std::function<void(const PartitionMapChange&)> callback) {
  std::unique_lock<std::shared_mutex> lock(watch_mutex_);
  watch_callbacks_.push_back(std::move(callback));
}

StatusOr<PartitionRoute> UnifiedMetaClient::GetRouteForDTx(
    PartitionID partition_id) {
  UnifiedPartitionInfo info;
  Status s = GetPartitionRoute(partition_id, &info);
  if (!s.ok()) return s;

  PartitionRoute route;
  route.partition_id = info.partition_id;
  route.leader_node = info.leader_node;
  route.leader_address = info.leader_address;
  route.version = info.version;
  route.cached_at = info.updated_at;
  return route;
}

UnifiedMetaClient::CacheStats UnifiedMetaClient::GetCacheStats() const {
  std::shared_lock<std::shared_mutex> lock(state_mutex_);
  CacheStats stats;
  stats.cached_partitions = partition_map_.size();
  stats.node_count = node_addresses_.size();
  stats.cluster_version = cluster_version_;
  stats.last_refresh = last_refresh_;
  return stats;
}

void UnifiedMetaClient::RefreshLoop() {
  int consecutive_failures = 0;
  while (running_) {
    {
      std::unique_lock<std::mutex> lock(shutdown_cv_mutex_);
      shutdown_cv_.wait_for(lock, options_.refresh_interval,
                            [this]() { return !running_.load(); });
      if (!running_) break;
    }
    Status s = FetchPartitionMapFromMeta();
    if (!s.ok()) {
      consecutive_failures++;
      LOG(WARNING) << "Partition map refresh failed (attempt " << consecutive_failures
                   << "): " << s.ToString();
      // Exponential backoff: double sleep on consecutive failures, max 5 minutes
      if (consecutive_failures > 1) {
        auto backoff = std::min(options_.refresh_interval * (1 << std::min(consecutive_failures - 1, 6)),
                                std::chrono::seconds(300));
        std::unique_lock<std::mutex> lock(shutdown_cv_mutex_);
        shutdown_cv_.wait_for(lock, backoff,
                              [this]() { return !running_.load(); });
      }
    } else {
      consecutive_failures = 0;
    }
  }
}

void UnifiedMetaClient::WatchLoop() {
  if (!channel_) return;

  auto stub = cedar::meta::MetaService::NewStub(channel_);

  while (running_) {
    grpc::ClientContext context;
    {
      std::lock_guard<std::mutex> lock(watch_context_mutex_);
      active_watch_context_ = &context;
    }
    cedar::meta::WatchPartitionMapRequest request;
    request.set_space_name(options_.space_name);

    {
      std::shared_lock<std::shared_mutex> lock(state_mutex_);
      request.set_current_version(cluster_version_);
    }

    auto stream = stub->WatchPartitionMap(&context, request);
    if (!stream) {
      {
        std::lock_guard<std::mutex> lock(watch_context_mutex_);
        if (active_watch_context_ == &context) {
          active_watch_context_ = nullptr;
        }
      }
      std::unique_lock<std::mutex> lock(shutdown_cv_mutex_);
      shutdown_cv_.wait_for(lock, seconds(5),
                            [this]() { return !running_.load(); });
      continue;
    }

    cedar::meta::PartitionMapChange change_proto;
    while (running_ && stream->Read(&change_proto)) {
      PartitionMapChange change;
      change.space_name = change_proto.space_name();
      change.partition_id =
          static_cast<PartitionID>(change_proto.partition_id());
      change.version = change_proto.version();

      const std::string& type = change_proto.change_type();
      if (type == "LEADER_CHANGED") {
        change.change_type = PartitionChangeType::kLeaderChanged;
      } else if (type == "REPLICA_ADDED") {
        change.change_type = PartitionChangeType::kReplicaAdded;
      } else if (type == "REPLICA_REMOVED") {
        change.change_type = PartitionChangeType::kReplicaRemoved;
      } else if (type == "PARTITION_MIGRATED") {
        change.change_type = PartitionChangeType::kPartitionMigrated;
      }
      change.old_leader = static_cast<NodeID>(change_proto.old_leader());
      change.new_leader = static_cast<NodeID>(change_proto.new_leader());

      ApplyPartitionChange(change);
    }

    {
      std::lock_guard<std::mutex> lock(watch_context_mutex_);
      if (active_watch_context_ == &context) {
        active_watch_context_ = nullptr;
      }
    }
    if (!running_) break;
    std::unique_lock<std::mutex> lock(shutdown_cv_mutex_);
    shutdown_cv_.wait_for(lock, seconds(2),
                          [this]() { return !running_.load(); });
  }
}

Status UnifiedMetaClient::FetchPartitionMapFromMeta() {
  if (!channel_) {
    return Status::IOError("Channel not initialized");
  }

  auto stub = cedar::meta::MetaService::NewStub(channel_);

  // Fetch partition map
  cedar::meta::GetSpacePartitionMapRequest pm_request;
  pm_request.set_space_name(options_.space_name);
  cedar::meta::GetSpacePartitionMapResponse pm_response;
  grpc::ClientContext pm_ctx;
  pm_ctx.set_deadline(std::chrono::system_clock::now() + options_.rpc_timeout);

  grpc::Status grpc_status =
      stub->GetSpacePartitionMap(&pm_ctx, pm_request, &pm_response);
  if (!grpc_status.ok()) {
    return Status::IOError("GetSpacePartitionMap failed: " +
                           grpc_status.error_message());
  }
  if (!pm_response.success()) {
    return Status::IOError("GetSpacePartitionMap rejected: " +
                           pm_response.error_msg());
  }

  // Fetch alive nodes for address resolution
  std::unordered_map<NodeID, std::string> addresses;
  Status nas = FetchAliveNodes(&addresses);
  if (!nas.ok()) return nas;

  // Build new partition map
  std::unordered_map<PartitionID, UnifiedPartitionInfo> new_map;
  auto now = steady_clock::now();

  for (const auto& kv : pm_response.partition_map().assignments()) {
    const auto& assign = kv.second;
    UnifiedPartitionInfo info;
    info.partition_id = static_cast<PartitionID>(assign.partition_id());
    info.leader_node = static_cast<NodeID>(assign.leader_node());
    info.version = assign.version();
    info.updated_at = now;

    auto it = addresses.find(info.leader_node);
    if (it != addresses.end()) {
      info.leader_address = it->second;
    } else {
      LOG(WARNING) << "Partition " << info.partition_id
                   << " references unknown leader node " << info.leader_node
                   << ", skipping";
      continue;
    }

    for (int i = 0; i < assign.follower_nodes_size(); ++i) {
      NodeID fid = static_cast<NodeID>(assign.follower_nodes(i));
      info.follower_nodes.push_back(fid);
      auto fit = addresses.find(fid);
      if (fit != addresses.end()) {
        info.follower_addresses.push_back(fit->second);
      }
    }

    new_map[info.partition_id] = std::move(info);
  }

  // Swap into cache under write lock
  {
    std::unique_lock<std::shared_mutex> lock(state_mutex_);
    partition_map_.swap(new_map);
    node_addresses_.swap(addresses);
    cluster_version_ = pm_response.partition_map().version();
    last_refresh_ = now;
  }

  return Status::OK();
}

Status UnifiedMetaClient::FetchAliveNodes(
    std::unordered_map<NodeID, std::string>* out) {
  auto stub = cedar::meta::MetaService::NewStub(channel_);
  cedar::meta::GetAliveNodesRequest request;
  cedar::meta::GetAliveNodesResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + options_.rpc_timeout);

  grpc::Status status = stub->GetAliveNodes(&context, request, &response);
  if (!status.ok()) {
    return Status::IOError("GetAliveNodes failed: " + status.error_message());
  }
  if (!response.success()) {
    return Status::IOError("GetAliveNodes rejected: " + response.error_msg());
  }

  for (const auto& node : response.nodes()) {
    (*out)[static_cast<NodeID>(node.node_id())] = node.address();
  }
  return Status::OK();
}

void UnifiedMetaClient::ApplyPartitionChange(const PartitionMapChange& change) {
  if (change.space_name != options_.space_name &&
      !change.space_name.empty()) {
    return;
  }

  switch (change.change_type) {
    case PartitionChangeType::kLeaderChanged: {
      std::unique_lock<std::shared_mutex> lock(state_mutex_);
      auto it = partition_map_.find(change.partition_id);
      if (it != partition_map_.end()) {
        auto addr_it = node_addresses_.find(change.new_leader);
        if (addr_it != node_addresses_.end()) {
          it->second.leader_node = change.new_leader;
          it->second.leader_address = addr_it->second;
          it->second.version = change.version;
          it->second.updated_at = steady_clock::now();
        } else {
          // New leader address unknown — invalidate so next read refreshes
          partition_map_.erase(it);
          LOG(WARNING) << "Leader changed for partition " << change.partition_id
                       << " but new leader " << change.new_leader
                       << " not in address map, invalidating";
        }
      }
      break;
    }
    case PartitionChangeType::kReplicaAdded:
    case PartitionChangeType::kReplicaRemoved:
    case PartitionChangeType::kPartitionMigrated:
      InvalidatePartition(change.partition_id);
      break;
  }

  std::vector<std::function<void(const PartitionMapChange&)>> callbacks;
  {
    std::shared_lock<std::shared_mutex> lock(watch_mutex_);
    callbacks = watch_callbacks_;
  }

  for (const auto& cb : callbacks) {
    try {
      cb(change);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Partition watch callback exception: " << e.what();
    } catch (...) {
      LOG(WARNING) << "Partition watch callback unknown exception";
    }
  }
}

}  // namespace dtx
}  // namespace cedar
