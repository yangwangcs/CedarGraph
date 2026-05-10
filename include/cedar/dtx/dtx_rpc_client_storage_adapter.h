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
// DTX RPC Client Storage Adapter
// Adapter that uses DTXRpcClient for 2PC protocol
// =============================================================================

#ifndef CEDAR_DTX_DTX_RPC_CLIENT_STORAGE_ADAPTER_H_
#define CEDAR_DTX_DTX_RPC_CLIENT_STORAGE_ADAPTER_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <mutex>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/dtx_rpc_client.h"

namespace cedar {
namespace dtx {

// =============================================================================
// Storage Client Interface
// =============================================================================

class StorageClientInterface {
 public:
  virtual ~StorageClientInterface() = default;
  virtual Status Put(PartitionID partition_id, uint64_t entity_id, const void* value, size_t size) = 0;
  virtual Status Get(PartitionID partition_id, uint64_t entity_id, void* value, size_t* size) = 0;
};

// =============================================================================
// DTX RPC Client Storage Adapter
// =============================================================================

class DTXRpcClientStorageAdapter : public StorageClientInterface {
 public:
  explicit DTXRpcClientStorageAdapter(std::shared_ptr<DTXRpcClient> dtx_client);
  ~DTXRpcClientStorageAdapter() override = default;

  Status Initialize(const std::unordered_map<PartitionID, NodeID>& partition_to_node);

  // StorageClientInterface implementation
  Status Put(PartitionID partition_id, uint64_t entity_id, const void* value, size_t size) override;
  Status Get(PartitionID partition_id, uint64_t entity_id, void* value, size_t* size) override;

  // DTX-specific operations
  Status Prepare(NodeID participant_id,
                 const std::string& txn_id,
                 uint64_t prepare_version,
                 cedar::dtx::PrepareResponse* response);

  Status Commit(NodeID participant_id,
                const std::string& txn_id,
                uint64_t commit_version,
                cedar::dtx::CommitResponse* response);

  Status Abort(NodeID participant_id,
               const std::string& txn_id,
               const std::string& reason,
               cedar::dtx::AbortResponse* response);

  DTXRpcClient* GetDTXRpcClient() const { return dtx_client_.get(); }

 private:
  NodeID GetNodeForPartition(PartitionID partition_id);

  std::shared_ptr<DTXRpcClient> dtx_client_;
  std::unordered_map<PartitionID, NodeID> partition_to_node_;
  mutable std::mutex mutex_;
};

// =============================================================================
// Inline Implementations
// =============================================================================

inline NodeID DTXRpcClientStorageAdapter::GetNodeForPartition(PartitionID partition_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = partition_to_node_.find(partition_id);
  if (it != partition_to_node_.end()) {
    return it->second;
  }
  return partition_id;
}

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_DTX_RPC_CLIENT_STORAGE_ADAPTER_H_
