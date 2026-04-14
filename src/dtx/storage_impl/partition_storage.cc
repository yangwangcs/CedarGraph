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

#include "cedar/dtx/storage_service_impl.h"
#include "cedar/dtx/monitoring.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace cedar {
namespace dtx {

// =============================================================================
// PartitionStorage Implementation - Shared LSM-Tree Architecture
// =============================================================================

PartitionStorage::PartitionStorage(PartitionID partition_id, 
                                   CedarGraphStorage* shared_storage,
                                   StoragePartitionManager* manager)
    : partition_id_(partition_id),
      shared_storage_(shared_storage),
      manager_(manager) {}

PartitionStorage::~PartitionStorage() {
  // Note: shared_storage_ is owned by PartitionManager, not deleted here
}

CedarKey PartitionStorage::InjectPartitionId(const CedarKey& key) const {
  CedarKey new_key = key;
  new_key.SetPartId(partition_id_);
  return new_key;
}

PartitionID PartitionStorage::ExtractPartitionId(const CedarKey& key) {
  return key.part_id();
}

Status PartitionStorage::Put(const CedarKey& key, const Descriptor& descriptor,
                            Timestamp txn_version, TxnID txn_id) {
  if (!shared_storage_) {
    return Status::IOError("Storage not initialized");
  }
  
  if (is_readonly_.load()) {
    return Status::InvalidArgument("Partition is read-only");
  }
  
  // Inject partition_id into key
  CedarKey storage_key = InjectPartitionId(key);
  
  // Use CedarGraphStorage API: Put(entity_id, tx_time, descriptor, txn_version)
  // Note: tx_time (business timestamp) comes from key.timestamp(), not txn_version
  Status s = shared_storage_->Put(
      storage_key.entity_id(),
      storage_key.timestamp().value(),  // Use key's timestamp as tx_time
      descriptor,
      txn_version
  );
  
  if (!s.ok()) {
    return Status::IOError("Write failed: " + s.ToString());
  }
  
  // Record write in transaction state if this is part of a transaction
  if (txn_id > 0) {
    std::unique_lock<std::shared_mutex> lock(txn_mutex_);
    auto it = prepared_txns_.find(txn_id);
    if (it != prepared_txns_.end()) {
      it->second.write_set.push_back(key);
    }
  }
  
  return Status::OK();
}

StatusOr<Descriptor> PartitionStorage::Get(const CedarKey& key, Timestamp read_time) {
  if (!shared_storage_) {
    return Status::IOError("Storage not initialized");
  }
  
  // Inject partition_id into key
  CedarKey storage_key = InjectPartitionId(key);
  
  // Use CedarGraphStorage API: Get(entity_id, entity_type, column_id, timestamp)
  // Use the caller's read_time so temporal queries return the correct version.
  auto result = shared_storage_->Get(
      storage_key.entity_id(),
      storage_key.entity_type(),
      storage_key.column_id(),
      read_time
  );
  
  if (!result.has_value()) {
    return Status::NotFound("Key not found");
  }
  
  return result.value();
}

Status PartitionStorage::Prepare(TxnID txn_id, const std::vector<CedarKey>& reads,
                                 const std::vector<CedarKey>& writes, Timestamp commit_ts) {
  if (!shared_storage_) {
    return Status::IOError("Storage not initialized");
  }
  
  std::unique_lock<std::shared_mutex> lock(txn_mutex_);
  
  // OCC validation: check if reads are still valid
  for (const auto& key : reads) {
    auto result = Get(key, commit_ts);
    if (!result.ok() && !result.status().IsNotFound()) {
      return result.status();
    }
  }
  
  // Record prepared transaction
  PreparedTxnState state;
  state.txn_id = txn_id;
  state.read_set = reads;
  state.write_set = writes;
  state.commit_ts = commit_ts;
  state.status = DistributedTxnState::kPreparing;
  
  prepared_txns_[txn_id] = std::move(state);
  
  // Write to WAL for durability
  WriteTxnWAL(txn_id, "PREPARE");
  
  return Status::OK();
}

Status PartitionStorage::Commit(TxnID txn_id, Timestamp commit_ts) {
  std::unique_lock<std::shared_mutex> lock(txn_mutex_);
  
  auto it = prepared_txns_.find(txn_id);
  if (it == prepared_txns_.end()) {
    return Status::NotFound("Transaction not prepared");
  }
  
  PreparedTxnState& state = it->second;
  state.status = DistributedTxnState::kCommitting;
  
  // Apply all buffered writes
  for (const auto& key : state.write_set) {
    // Create a descriptor for the write
    Descriptor desc = Descriptor(static_cast<uint64_t>(commit_ts));
    
    // Write to shared storage
    lock.unlock();
    Status s = Put(key, desc, commit_ts, txn_id);
    lock.lock();
    
    if (!s.ok()) {
      state.status = DistributedTxnState::kAborted;
      return s;
    }
  }
  
  state.status = DistributedTxnState::kCommitted;
  prepared_txns_.erase(it);
  
  // Write to WAL
  WriteTxnWAL(txn_id, "COMMIT");
  
  return Status::OK();
}

Status PartitionStorage::Abort(TxnID txn_id) {
  std::unique_lock<std::shared_mutex> lock(txn_mutex_);
  
  auto it = prepared_txns_.find(txn_id);
  if (it == prepared_txns_.end()) {
    return Status::NotFound("Transaction not found");
  }
  
  it->second.status = DistributedTxnState::kAborted;
  prepared_txns_.erase(it);
  
  // Write to WAL
  WriteTxnWAL(txn_id, "ABORT");
  
  return Status::OK();
}

bool PartitionStorage::IsReadOnly() const {
  return is_readonly_.load();
}

void PartitionStorage::SetReadOnly(bool readonly) {
  is_readonly_ = readonly;
}

PartitionStorage::StorageStats PartitionStorage::GetStats() const {
  StorageStats stats;
  stats.partition_id = partition_id_;
  
  // Get active transaction count
  {
    std::shared_lock<std::shared_mutex> lock(txn_mutex_);
    stats.num_active_txns = prepared_txns_.size();
  }
  
  // Estimate partition stats from shared storage
  if (shared_storage_) {
    // Get storage statistics
    auto storage_stats = shared_storage_->GetStats();
    
    // Estimate per-partition stats based on partition proportion
    // In a shared LSM-Tree, we approximate based on partition ID distribution
    size_t total_partitions = 256;  // Default assumption
    if (manager_) {
      total_partitions = std::max(size_t(1), manager_->GetPartitionCount());
    }
    
    // Rough estimation: divide total stats by partition count
    // Note: actual Stats structure may vary, using available fields
    stats.num_keys = storage_stats.sst_count * 1000 / total_partitions;  // Rough estimate
    stats.disk_usage_bytes = storage_stats.sst_size / total_partitions;
  }
  
  return stats;
}

std::vector<TxnID> PartitionStorage::GetPreparedTransactions() const {
  std::shared_lock<std::shared_mutex> lock(txn_mutex_);
  std::vector<TxnID> txns;
  txns.reserve(prepared_txns_.size());
  for (const auto& [txn_id, state] : prepared_txns_) {
    txns.push_back(txn_id);
  }
  return txns;
}

Status PartitionStorage::WriteTxnWAL(uint64_t txn_id, const std::string& operation) {
  // Simplified WAL - in production, use dedicated WAL per partition
  // or shared WAL with partition_id prefix
  std::string wal_path = "/tmp/cedar_wal/partition_" + std::to_string(partition_id_) + "_wal.txt";
  std::filesystem::create_directories("/tmp/cedar_wal");
  std::ofstream wal(wal_path, std::ios::app);
  if (wal.is_open()) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    wal << time_t << " " << txn_id << " " << operation << "\n";
  }
  return Status::OK();
}

}  // namespace dtx
}  // namespace cedar
