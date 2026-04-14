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
// DTx RPC Client - Stub Implementation
// =============================================================================
// Note: This is a stub implementation. Real gRPC client requires protobuf fix.
// =============================================================================

#ifndef CEDAR_DTX_RPC_CLIENT_H_
#define CEDAR_DTX_RPC_CLIENT_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// Forward declaration for governance layer integration
namespace governance {
  class ServiceRegistry;
}

namespace dtx {

// Stub RPC client for distributed transactions
class DTxRpcClient {
 public:
  explicit DTxRpcClient(const DTxConfig& config);
  ~DTxRpcClient();
  
  // Node management
  Status AddNode(NodeID node_id, const std::string& address);
  void RemoveNode(NodeID node_id);
  bool IsNodeAvailable(NodeID node_id);
  
  // Service discovery integration (governance layer)
  Status DiscoverAndAddNodes(const std::string& service_name,
                              governance::ServiceRegistry& registry);
  void RefreshNodesFromRegistry(const std::string& service_name,
                                 governance::ServiceRegistry& registry);
  std::vector<NodeID> GetDiscoveredNodes() const;
  
  // Data operations
  Status Put(NodeID node_id, PartitionID pid, const CedarKey& key, 
             const Descriptor& value, Timestamp txn_version);
  StatusOr<Descriptor> Get(NodeID node_id, PartitionID pid, 
                           const CedarKey& key, Timestamp read_time);
  
  // 2PC operations
  Status Prepare(NodeID node_id, TxnID txn_id, 
                 const std::vector<CedarKey>& reads,
                 const std::vector<CedarKey>& writes, 
                 Timestamp commit_ts);
  Status Commit(NodeID node_id, TxnID txn_id, Timestamp commit_ts);
  Status Abort(NodeID node_id, TxnID txn_id);

 private:
  DTxConfig config_;
  
  struct NodeInfo {
    NodeID id;
    std::string address;
    std::atomic<bool> available{true};
    std::string service_id;  // From ServiceRegistry for tracking
  };
  
  mutable std::mutex mutex_;
  std::unordered_map<NodeID, std::unique_ptr<NodeInfo>> nodes_;
  
  // Watch handle for service registry updates (from governance layer)
  int64_t registry_watch_id_ = -1;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_RPC_CLIENT_H_
