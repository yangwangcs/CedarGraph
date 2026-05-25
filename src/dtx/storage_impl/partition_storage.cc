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
#include "cedar/storage/lsm_engine.h"

#include <chrono>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sys/uio.h>
#include <unistd.h>

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
                                 const std::vector<CedarKey>& writes,
                                 const std::unordered_map<uint64_t, Descriptor>& write_descriptors,
                                 Timestamp commit_ts) {
  if (!shared_storage_) {
    return Status::IOError("Storage not initialized");
  }
  
  std::unique_lock<std::shared_mutex> lock(txn_mutex_);
  
  // Check if transaction already exists
  if (prepared_txns_.find(txn_id) != prepared_txns_.end()) {
    return Status::InvalidArgument("Transaction already prepared: " + std::to_string(txn_id));
  }
  
  // Conflict detection: check if any write conflicts with existing prepared transactions
  for (const auto& [existing_txn_id, existing_state] : prepared_txns_) {
    if (existing_state.status != DistributedTxnState::kPrepared) {
      continue;
    }
    
    // Check write-write conflicts
    for (const auto& write_key : writes) {
      for (const auto& existing_write : existing_state.write_set) {
        if (write_key.entity_id() == existing_write.entity_id() &&
            write_key.column_id() == existing_write.column_id()) {
          return Status::Busy("Write-write conflict with txn " + std::to_string(existing_txn_id));
        }
      }
    }
  }
  
  // Validate read-set: check that no key in reads has been modified
  // after the commit_ts (i.e., by a later committed transaction).
  // We check the LSM memtables where txn_version is preserved.
  // SST files currently do not store txn_version in a retrievable way,
  // so this is a best-effort check that catches recent conflicts.
  if (shared_storage_) {
    auto* lsm_engine = shared_storage_->GetLsmEngine();
    if (lsm_engine) {
      for (const auto& read_key : reads) {
        if (ExtractPartitionId(read_key) != partition_id_) {
          continue;
        }
        auto versions = lsm_engine->GetAll(
            read_key.entity_id(), read_key.entity_type(), read_key.column_id());
        for (const auto& entry : versions) {
          // SST entries have txn_version == 0 (not preserved during flush)
          if (entry.txn_version == Timestamp(0)) {
            continue;
          }
          if (entry.txn_version > commit_ts) {
            return Status::Busy("Read-write conflict: key modified after read timestamp");
          }
        }
      }
    }
  }
  
  PreparedTxnState state;
  state.txn_id = txn_id;
  state.read_set = reads;
  state.write_set = writes;
  state.write_descriptors = write_descriptors;
  state.commit_ts = commit_ts;
  state.status = DistributedTxnState::kPrepared;
  prepared_txns_[txn_id] = std::move(state);
  
  // Write WAL for durability
  WriteTxnWAL(txn_id, "PREPARE");
  
  return Status::OK();
}

Status PartitionStorage::Commit(TxnID txn_id, Timestamp commit_ts) {
  if (!shared_storage_) {
    return Status::IOError("Storage not initialized");
  }
  
  if (is_readonly_.load()) {
    return Status::InvalidArgument("Partition is read-only");
  }
  
  std::unique_lock<std::shared_mutex> lock(txn_mutex_);
  
  auto it = prepared_txns_.find(txn_id);
  if (it == prepared_txns_.end()) {
    return Status::NotFound("Transaction not found: " + std::to_string(txn_id));
  }
  
  auto& state = it->second;
  if (state.status != DistributedTxnState::kPrepared) {
    return Status::InvalidArgument("Transaction not in prepared state: " + std::to_string(txn_id));
  }
  
  struct WriteEntry {
    CedarKey key;
    Descriptor desc;
  };
  std::vector<WriteEntry> validated_writes;
  validated_writes.reserve(state.write_set.size());

  for (const auto& key : state.write_set) {
    if (ExtractPartitionId(key) != partition_id_) {
      continue;
    }
    Descriptor desc;
    uint64_t key_hash = static_cast<uint64_t>(cedar::dtx::CedarKeyHash{}(key));
    auto desc_it = state.write_descriptors.find(key_hash);
    if (desc_it != state.write_descriptors.end()) {
      desc = desc_it->second;
    }
    validated_writes.push_back({key, desc});
  }

  size_t written_count = 0;
  for (const auto& entry : validated_writes) {
    CedarKey storage_key = InjectPartitionId(entry.key);
    Status s = shared_storage_->Put(
        storage_key.entity_id(),
        storage_key.timestamp().value(),
        entry.desc,
        commit_ts
    );
    if (!s.ok()) {
      std::cerr << "[PartitionStorage::Commit] PARTIAL_WRITE txn_id=" << txn_id
                << " partition=" << partition_id_
                << " written=" << written_count
                << " total=" << validated_writes.size()
                << " error=" << s.ToString() << std::endl;
      state.status = DistributedTxnState::kCommitting;
      return Status::IOError(
          "Partial write during commit — recovery will complete remaining keys: " +
          s.ToString());
    }
    written_count++;
  }

  state.status = DistributedTxnState::kCommitted;
  WriteTxnWAL(txn_id, "COMMIT");
  prepared_txns_.erase(it);
  
  return Status::OK();
}

Status PartitionStorage::Abort(TxnID txn_id) {
  std::unique_lock<std::shared_mutex> lock(txn_mutex_);
  
  auto it = prepared_txns_.find(txn_id);
  if (it == prepared_txns_.end()) {
    return Status::NotFound("Transaction not found: " + std::to_string(txn_id));
  }
  
  it->second.status = DistributedTxnState::kAborted;
  
  // Write WAL
  WriteTxnWAL(txn_id, "ABORT");
  
  // Remove from prepared transactions
  prepared_txns_.erase(it);
  
  return Status::OK();
}

Status PartitionStorage::Inquire(TxnID txn_id, DistributedTxnState* state) {
  std::shared_lock<std::shared_mutex> lock(txn_mutex_);
  
  auto it = prepared_txns_.find(txn_id);
  if (it == prepared_txns_.end()) {
    return Status::NotFound("Transaction not found: " + std::to_string(txn_id));
  }
  
  *state = it->second.status;
  return Status::OK();
}

bool PartitionStorage::IsReadOnly() const {
  return is_readonly_.load();
}

void PartitionStorage::SetReadOnly(bool readonly) {
  is_readonly_.store(readonly);
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

// =============================================================================
// Snapshot Support - Serialize/Deserialize Prepared Transaction State
// =============================================================================

// Binary format:
// [ magic: 4 ] = "CTSN"
// [ version: 4 ] = 1
// [ num_txns: 4 ]
// for each txn:
//   [ txn_id: 8 ]
//   [ commit_ts: 8 ]
//   [ status: 4 ]
//   [ num_reads: 4 ]
//   for each read: [ CedarKey raw: 32 ]
//   [ num_writes: 4 ]
//   for each write: [ CedarKey raw: 32 ]
//   [ num_descriptors: 4 ]
//   for each descriptor: [ key_hash: 8 ][ descriptor_raw: 8 ]

static constexpr char kTxnSnapshotMagic[] = "CTSN";
static constexpr uint32_t kTxnSnapshotVersion = 1;

Status PartitionStorage::SavePreparedTxns(const std::string& path) const {
  std::ofstream file(path, std::ios::binary);
  if (!file) {
    return Status::IOError("Failed to open snapshot file for writing: " + path);
  }

  // Write magic
  file.write(kTxnSnapshotMagic, 4);
  if (!file) return Status::IOError("Failed to write magic");

  // Write version
  uint32_t version = kTxnSnapshotVersion;
  file.write(reinterpret_cast<const char*>(&version), sizeof(version));
  if (!file) return Status::IOError("Failed to write version");

  std::shared_lock<std::shared_mutex> lock(txn_mutex_);

  // Write transaction count
  uint32_t num_txns = static_cast<uint32_t>(prepared_txns_.size());
  file.write(reinterpret_cast<const char*>(&num_txns), sizeof(num_txns));
  if (!file) return Status::IOError("Failed to write txn count");

  for (const auto& [txn_id, state] : prepared_txns_) {
    // txn_id
    uint64_t tid = txn_id;
    file.write(reinterpret_cast<const char*>(&tid), sizeof(tid));
    if (!file) return Status::IOError("Failed to write txn_id");

    // commit_ts
    uint64_t cts = state.commit_ts.value();
    file.write(reinterpret_cast<const char*>(&cts), sizeof(cts));
    if (!file) return Status::IOError("Failed to write commit_ts");

    // status
    uint32_t st = static_cast<uint32_t>(state.status);
    file.write(reinterpret_cast<const char*>(&st), sizeof(st));
    if (!file) return Status::IOError("Failed to write status");

    // read_set
    uint32_t num_reads = static_cast<uint32_t>(state.read_set.size());
    file.write(reinterpret_cast<const char*>(&num_reads), sizeof(num_reads));
    if (!file) return Status::IOError("Failed to write read count");
    for (const auto& key : state.read_set) {
      // CedarKey is a POD-like struct of 32 bytes
      file.write(reinterpret_cast<const char*>(&key), sizeof(key));
      if (!file) return Status::IOError("Failed to write read key");
    }

    // write_set
    uint32_t num_writes = static_cast<uint32_t>(state.write_set.size());
    file.write(reinterpret_cast<const char*>(&num_writes), sizeof(num_writes));
    if (!file) return Status::IOError("Failed to write write count");
    for (const auto& key : state.write_set) {
      file.write(reinterpret_cast<const char*>(&key), sizeof(key));
      if (!file) return Status::IOError("Failed to write write key");
    }

    // write_descriptors
    uint32_t num_descs = static_cast<uint32_t>(state.write_descriptors.size());
    file.write(reinterpret_cast<const char*>(&num_descs), sizeof(num_descs));
    if (!file) return Status::IOError("Failed to write descriptor count");
    for (const auto& [key_hash, desc] : state.write_descriptors) {
      file.write(reinterpret_cast<const char*>(&key_hash), sizeof(key_hash));
      if (!file) return Status::IOError("Failed to write key_hash");
      uint64_t desc_raw = desc.AsRaw();
      file.write(reinterpret_cast<const char*>(&desc_raw), sizeof(desc_raw));
      if (!file) return Status::IOError("Failed to write descriptor raw");
    }
  }

  file.flush();
  return Status::OK();
}

Status PartitionStorage::LoadPreparedTxns(const std::string& path) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    return Status::IOError("Failed to open snapshot file for reading: " + path);
  }

  // Read magic
  char magic[4];
  file.read(magic, 4);
  if (!file || memcmp(magic, kTxnSnapshotMagic, 4) != 0) {
    return Status::InvalidArgument("Invalid snapshot file magic");
  }

  // Read version
  uint32_t version;
  file.read(reinterpret_cast<char*>(&version), sizeof(version));
  if (!file || version != kTxnSnapshotVersion) {
    return Status::InvalidArgument("Unsupported snapshot version: " + std::to_string(version));
  }

  // Read transaction count
  uint32_t num_txns;
  file.read(reinterpret_cast<char*>(&num_txns), sizeof(num_txns));
  if (!file) return Status::IOError("Failed to read txn count");

  std::unique_lock<std::shared_mutex> lock(txn_mutex_);
  prepared_txns_.clear();

  for (uint32_t i = 0; i < num_txns; ++i) {
    PreparedTxnState state;

    // txn_id
    uint64_t txn_id;
    file.read(reinterpret_cast<char*>(&txn_id), sizeof(txn_id));
    if (!file) return Status::IOError("Failed to read txn_id");
    state.txn_id = txn_id;

    // commit_ts
    uint64_t cts;
    file.read(reinterpret_cast<char*>(&cts), sizeof(cts));
    if (!file) return Status::IOError("Failed to read commit_ts");
    state.commit_ts = Timestamp(cts);

    // status
    uint32_t st;
    file.read(reinterpret_cast<char*>(&st), sizeof(st));
    if (!file) return Status::IOError("Failed to read status");
    state.status = static_cast<DistributedTxnState>(st);

    // read_set
    uint32_t num_reads;
    file.read(reinterpret_cast<char*>(&num_reads), sizeof(num_reads));
    if (!file) return Status::IOError("Failed to read read count");
    state.read_set.reserve(num_reads);
    for (uint32_t r = 0; r < num_reads; ++r) {
      CedarKey key;
      file.read(reinterpret_cast<char*>(&key), sizeof(key));
      if (!file) return Status::IOError("Failed to read read key");
      state.read_set.push_back(key);
    }

    // write_set
    uint32_t num_writes;
    file.read(reinterpret_cast<char*>(&num_writes), sizeof(num_writes));
    if (!file) return Status::IOError("Failed to read write count");
    state.write_set.reserve(num_writes);
    for (uint32_t w = 0; w < num_writes; ++w) {
      CedarKey key;
      file.read(reinterpret_cast<char*>(&key), sizeof(key));
      if (!file) return Status::IOError("Failed to read write key");
      state.write_set.push_back(key);
    }

    // write_descriptors
    uint32_t num_descs;
    file.read(reinterpret_cast<char*>(&num_descs), sizeof(num_descs));
    if (!file) return Status::IOError("Failed to read descriptor count");
    for (uint32_t d = 0; d < num_descs; ++d) {
      uint64_t key_hash;
      file.read(reinterpret_cast<char*>(&key_hash), sizeof(key_hash));
      if (!file) return Status::IOError("Failed to read key_hash");
      uint64_t desc_raw;
      file.read(reinterpret_cast<char*>(&desc_raw), sizeof(desc_raw));
      if (!file) return Status::IOError("Failed to read descriptor raw");
      state.write_descriptors[key_hash] = Descriptor(desc_raw);
    }

    prepared_txns_[txn_id] = std::move(state);
  }

  return Status::OK();
}

std::string PartitionStorage::GetDataRoot() const {
  return manager_ ? manager_->GetDataRoot() : "/tmp/cedar_storage";
}

Status PartitionStorage::SaveSnapshotForMigration(const std::string& snapshot_path) const {
  std::filesystem::create_directories(snapshot_path);

  auto data_root = GetDataRoot();
  for (const auto& entry : std::filesystem::recursive_directory_iterator(data_root)) {
    if (!entry.is_regular_file()) continue;
    auto rel_path = std::filesystem::relative(entry.path(), data_root);
    auto dst_path = snapshot_path + "/" + rel_path.string();
    std::filesystem::create_directories(std::filesystem::path(dst_path).parent_path());
    std::filesystem::copy_file(entry.path(), dst_path,
                                std::filesystem::copy_options::overwrite_existing);
  }

  auto txn_state_path = snapshot_path + "/txn_state";
  auto s = SavePreparedTxns(txn_state_path);
  if (!s.ok()) return s;

  return Status::OK();
}

Status PartitionStorage::RecoverFromWAL() {
  std::string wal_dir = manager_ ? manager_->GetDataRoot() + "/wal" : "/tmp/cedar_wal";
  std::string wal_path = wal_dir + "/partition_" + std::to_string(partition_id_) + "_wal.log";
  
  if (!std::filesystem::exists(wal_path)) {
    return Status::OK();  // No WAL to recover
  }
  
  int fd = ::open(wal_path.c_str(), O_RDONLY);
  if (fd < 0) {
    return Status::IOError("RecoverFromWAL", "Failed to open WAL file");
  }
  
  struct stat st;
  if (fstat(fd, &st) < 0 || st.st_size == 0) {
    ::close(fd);
    return Status::OK();
  }
  
  std::string wal_data(st.st_size, '\0');
  ssize_t n = ::read(fd, &wal_data[0], st.st_size);
  ::close(fd);
  if (n != st.st_size) {
    return Status::IOError("RecoverFromWAL", "Failed to read complete WAL");
  }
  
  // Parse WAL records: [timestamp:8][txn_id:8][op_len:4][operation]
  size_t pos = 0;
  std::unordered_map<TxnID, std::string> last_op_for_txn;
  
  while (pos + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t) <= static_cast<size_t>(st.st_size)) {
    uint64_t ts, txn_id;
    uint32_t op_len;
    
    std::memcpy(&ts, &wal_data[pos], sizeof(ts));
    pos += sizeof(ts);
    std::memcpy(&txn_id, &wal_data[pos], sizeof(txn_id));
    pos += sizeof(txn_id);
    std::memcpy(&op_len, &wal_data[pos], sizeof(op_len));
    pos += sizeof(op_len);
    
    if (pos + op_len > static_cast<size_t>(st.st_size)) {
      std::cerr << "[PartitionStorage] Corrupt WAL record at offset " << (pos - sizeof(ts) - sizeof(txn_id) - sizeof(op_len))
                << " for partition " << partition_id_ << std::endl;
      break;
    }
    
    std::string op = wal_data.substr(pos, op_len);
    pos += op_len;
    
    last_op_for_txn[txn_id] = op;
  }
  
  // Apply recovered operations
  size_t recovered_commits = 0;
  size_t recovered_aborts = 0;
  
  for (const auto& [txn_id, op] : last_op_for_txn) {
    if (op == "COMMIT") {
      // Best-effort: if transaction is still prepared, complete it
      auto s = Commit(txn_id, Timestamp::Max());
      if (s.ok() || s.IsNotFound()) {
        recovered_commits++;
      }
    } else if (op == "ABORT") {
      auto s = Abort(txn_id);
      if (s.ok() || s.IsNotFound()) {
        recovered_aborts++;
      }
    }
    // PREPARE records cannot be fully recovered because WAL does not store
    // read/write sets. They are handled by snapshot LoadPreparedTxns instead.
  }
  
  if (recovered_commits > 0 || recovered_aborts > 0) {
    std::cerr << "[PartitionStorage] Recovered " << recovered_commits << " commits and "
              << recovered_aborts << " aborts from WAL for partition " << partition_id_ << std::endl;
  }
  
  return Status::OK();
}

Status PartitionStorage::WriteTxnWAL(uint64_t txn_id, const std::string& operation) {
  std::string wal_dir = manager_ ? manager_->GetDataRoot() + "/wal" : "/tmp/cedar_wal";
  std::string wal_path = wal_dir + "/partition_" + std::to_string(partition_id_) + "_wal.log";
  std::filesystem::create_directories(wal_dir);
  
  // Use POSIX open for durable writes with O_APPEND
  int fd = ::open(wal_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) {
    return Status::IOError("WriteTxnWAL", "Failed to open WAL file");
  }
  
  // Format: [timestamp:8][txn_id:8][op_len:4][operation]
  auto now = std::chrono::system_clock::now();
  uint64_t ts = static_cast<uint64_t>(std::chrono::system_clock::to_time_t(now));
  uint64_t tid = txn_id;
  uint32_t op_len = static_cast<uint32_t>(operation.size());
  
  struct iovec iov[4];
  iov[0].iov_base = &ts;
  iov[0].iov_len = sizeof(ts);
  iov[1].iov_base = &tid;
  iov[1].iov_len = sizeof(tid);
  iov[2].iov_base = &op_len;
  iov[2].iov_len = sizeof(op_len);
  iov[3].iov_base = const_cast<char*>(operation.data());
  iov[3].iov_len = operation.size();
  
  ssize_t written = ::writev(fd, iov, 4);
  if (written < 0 || static_cast<size_t>(written) != sizeof(ts) + sizeof(tid) + sizeof(op_len) + operation.size()) {
    ::close(fd);
    return Status::IOError("WriteTxnWAL", "Partial write");
  }
  
  // Sync to disk
  #ifdef __APPLE__
    if (::fcntl(fd, F_FULLFSYNC) < 0) {
      ::close(fd);
      return Status::IOError("WriteTxnWAL", "F_FULLFSYNC failed");
    }
  #else
    if (::fsync(fd) < 0) {
      ::close(fd);
      return Status::IOError("WriteTxnWAL", "fsync failed");
    }
  #endif
  
  ::close(fd);
  return Status::OK();
}

}  // namespace dtx
}  // namespace cedar
