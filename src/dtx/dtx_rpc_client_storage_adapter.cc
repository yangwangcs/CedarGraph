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
// DTX RPC Client Storage Adapter Implementation
// =============================================================================

#include "cedar/dtx/dtx_rpc_client_storage_adapter.h"

#include <iostream>

namespace cedar {
namespace dtx {

DTXRpcClientStorageAdapter::DTXRpcClientStorageAdapter(
    std::shared_ptr<DTXRpcClient> dtx_client)
    : dtx_client_(dtx_client) {}

Status DTXRpcClientStorageAdapter::Initialize(
    const std::unordered_map<PartitionID, NodeID>& partition_to_node) {
  std::lock_guard<std::mutex> lock(mutex_);
  partition_to_node_ = partition_to_node;
  return Status::OK();
}

Status DTXRpcClientStorageAdapter::Put(PartitionID partition_id,
                                       uint64_t entity_id,
                                       const void* value,
                                       size_t size) {
  // Placeholder - real implementation would use StorageService RPC
  (void)partition_id;
  (void)entity_id;
  (void)value;
  (void)size;
  return Status::OK();
}

Status DTXRpcClientStorageAdapter::Get(PartitionID partition_id,
                                      uint64_t entity_id,
                                      void* value,
                                      size_t* size) {
  (void)partition_id;
  (void)entity_id;
  (void)value;
  (void)size;
  return Status::OK();
}

Status DTXRpcClientStorageAdapter::Prepare(
    NodeID participant_id,
    const std::string& txn_id,
    uint64_t prepare_version,
    cedar::dtx::PrepareResponse* response) {
  return dtx_client_->Prepare(
      participant_id, txn_id, "coordinator", prepare_version,
      {}, {}, 1, 5000, response);
}

Status DTXRpcClientStorageAdapter::Commit(
    NodeID participant_id,
    const std::string& txn_id,
    uint64_t commit_version,
    cedar::dtx::CommitResponse* response) {
  return dtx_client_->Commit(
      participant_id, txn_id, "coordinator", commit_version, response);
}

Status DTXRpcClientStorageAdapter::Abort(
    NodeID participant_id,
    const std::string& txn_id,
    const std::string& reason,
    cedar::dtx::AbortResponse* response) {
  return dtx_client_->Abort(
      participant_id, txn_id, "coordinator", reason, response);
}

}  // namespace dtx
}  // namespace cedar
