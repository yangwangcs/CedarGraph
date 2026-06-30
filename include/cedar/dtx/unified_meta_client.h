// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Unified Meta Client - Single routing cache for both QueryD and DTx coordinator

#ifndef CEDAR_DTX_UNIFIED_META_CLIENT_H_
#define CEDAR_DTX_UNIFIED_META_CLIENT_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>

#include "cedar/core/status.h"
#include "cedar/dtx/coordinator_integration.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {

struct UnifiedPartitionInfo {
  PartitionID partition_id{kInvalidPartitionID};
  NodeID leader_node{kInvalidNodeID};
  std::string leader_address;
  std::vector<NodeID> follower_nodes;
  std::vector<std::string> follower_addresses;
  uint64_t version{0};
  std::chrono::steady_clock::time_point updated_at;

  bool IsExpired(uint64_t ttl_sec = 60) const {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(
        now - updated_at).count() > static_cast<int64_t>(ttl_sec);
  }
};

class UnifiedMetaClient {
 public:
  struct Options {
    std::string meta_service_address;
    std::string space_name{"default"};
    std::chrono::seconds refresh_interval{30};
    std::chrono::milliseconds rpc_timeout{5000};
    uint64_t route_cache_ttl_sec{60};
    bool enable_watch{true};
  };

  explicit UnifiedMetaClient(const Options& options);
  ~UnifiedMetaClient();

  UnifiedMetaClient(const UnifiedMetaClient&) = delete;
  UnifiedMetaClient& operator=(const UnifiedMetaClient&) = delete;

  Status Init();
  Status Shutdown();

  Status GetLeaderAddress(PartitionID partition_id, std::string* address);
  Status GetLeaderNode(PartitionID partition_id, NodeID* node_id);
  Status GetFollowerAddresses(PartitionID partition_id,
                              std::vector<std::string>* addresses);
  Status GetPartitionRoute(PartitionID partition_id,
                           UnifiedPartitionInfo* info);

  void InvalidatePartition(PartitionID partition_id);
  void InvalidateAll();

  Status RefreshPartitionMap();

  void WatchPartitionMap(
      std::function<void(const PartitionMapChange&)> callback);

  StatusOr<PartitionRoute> GetRouteForDTx(PartitionID partition_id);

  struct CacheStats {
    size_t cached_partitions{0};
    size_t node_count{0};
    uint64_t cluster_version{0};
    std::chrono::steady_clock::time_point last_refresh;
  };
  CacheStats GetCacheStats() const;

  const std::string& space_name() const { return options_.space_name; }

 private:
  void RefreshLoop();
  void WatchLoop();
  Status FetchPartitionMapFromMeta();
  Status FetchAliveNodes(std::unordered_map<NodeID, std::string>* out);
  void ApplyPartitionChange(const PartitionMapChange& change);

  Options options_;

  std::shared_ptr<grpc::Channel> channel_;

  mutable std::shared_mutex state_mutex_;
  std::unordered_map<PartitionID, UnifiedPartitionInfo> partition_map_;
  std::unordered_map<NodeID, std::string> node_addresses_;
  uint64_t cluster_version_{0};
  std::chrono::steady_clock::time_point last_refresh_;

  mutable std::shared_mutex watch_mutex_;
  std::vector<std::function<void(const PartitionMapChange&)>> watch_callbacks_;

  std::atomic<bool> running_{false};
  std::thread refresh_thread_;
  std::thread watch_thread_;
  std::condition_variable shutdown_cv_;
  std::mutex shutdown_cv_mutex_;
  std::mutex watch_context_mutex_;
  grpc::ClientContext* active_watch_context_{nullptr};
};

// Adapter: wraps UnifiedMetaClient to satisfy MetaClientInterface
class UnifiedMetaClientAdapter : public MetaClientInterface {
 public:
  explicit UnifiedMetaClientAdapter(UnifiedMetaClient* client) : client_(client) {}

  std::string GetLeaderAddress(uint32_t partition_id) override {
    std::string address;
    Status s = client_->GetLeaderAddress(partition_id, &address);
    return s.ok() ? address : "";
  }

  void InvalidatePartition(uint32_t partition_id) override {
    client_->InvalidatePartition(partition_id);
  }

 private:
  UnifiedMetaClient* client_;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_UNIFIED_META_CLIENT_H_
