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
#include "cedar/cdc/partition_change_log.h"
#include "cedar/dtx/monitoring.h"
#include "cedar/storage/lsm_engine.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <system_error>
#include <sys/uio.h>
#include <unistd.h>

namespace cedar {
namespace dtx {

namespace {

namespace fs = std::filesystem;

constexpr char kCdcIntentMagic[] = "CEDAR_CDC_INTENT_V1";

Status FilesystemStatus(const std::string& operation,
                        const fs::path& path,
                        const std::error_code& ec) {
  return Status::IOError(operation + ": " + path.string(), ec.message());
}

bool IsSameOrChildPath(const fs::path& path, const fs::path& parent) {
  auto path_it = path.begin();
  auto parent_it = parent.begin();
  for (; parent_it != parent.end(); ++parent_it, ++path_it) {
    if (path_it == path.end() || *path_it != *parent_it) {
      return false;
    }
  }
  return true;
}

bool IsMigrationSnapshotDirectory(const fs::path& path) {
  const std::string name = path.filename().string();
  return name.rfind("migration_snap_", 0) == 0;
}

bool HasParentTraversal(const fs::path& path) {
  for (const auto& part : path) {
    if (part == "..") return true;
  }
  return false;
}

struct CdcIntentBatch {
  uint64_t txn_id = 0;
  uint64_t commit_version = 0;
  std::vector<std::pair<uint64_t, cedar::cdc::ChangeRecord>> records;
};

StatusOr<CdcIntentBatch> ReadCdcIntentFile(const fs::path& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return Status::IOError("ReadCdcIntentFile", "failed to open " + path.string());
  }

  uint32_t magic_size = 0;
  in.read(reinterpret_cast<char*>(&magic_size), sizeof(magic_size));
  if (!in || magic_size == 0 || magic_size > 1024) {
    return Status::Corruption("ReadCdcIntentFile", "invalid magic size");
  }
  std::string magic(magic_size, '\0');
  in.read(magic.data(), magic.size());
  if (!in || magic != kCdcIntentMagic) {
    return Status::Corruption("ReadCdcIntentFile", "invalid magic");
  }

  CdcIntentBatch batch;
  uint32_t record_count = 0;
  in.read(reinterpret_cast<char*>(&batch.txn_id), sizeof(batch.txn_id));
  in.read(reinterpret_cast<char*>(&batch.commit_version),
          sizeof(batch.commit_version));
  in.read(reinterpret_cast<char*>(&record_count), sizeof(record_count));
  if (!in || record_count > 1000000) {
    return Status::Corruption("ReadCdcIntentFile", "invalid record count");
  }

  batch.records.reserve(record_count);
  for (uint32_t i = 0; i < record_count; ++i) {
    uint64_t storage_timestamp = 0;
    in.read(reinterpret_cast<char*>(&storage_timestamp),
            sizeof(storage_timestamp));
    if (!in) {
      return Status::Corruption("ReadCdcIntentFile",
                                "invalid storage timestamp");
    }
    uint32_t record_size = 0;
    in.read(reinterpret_cast<char*>(&record_size), sizeof(record_size));
    if (!in || record_size == 0 || record_size > 16 * 1024 * 1024) {
      return Status::Corruption("ReadCdcIntentFile", "invalid record size");
    }
    std::string serialized(record_size, '\0');
    in.read(serialized.data(), serialized.size());
    cedar::cdc::ChangeRecord record;
    if (!in || !record.ParseFromString(serialized)) {
      return Status::Corruption("ReadCdcIntentFile", "invalid ChangeRecord");
    }
    batch.records.push_back({storage_timestamp, std::move(record)});
  }
  return batch;
}

}  // namespace

// =============================================================================
// PartitionStorage Implementation - Shared LSM-Tree Architecture
// =============================================================================

PartitionStorage::PartitionStorage(PartitionID partition_id,
                                   CedarGraphStorage* shared_storage,
                                   StoragePartitionManager* manager,
                                   StorageBackend* backend)
    : partition_id_(partition_id),
      shared_storage_(shared_storage),
      backend_(backend),
      manager_(manager) {}

PartitionStorage::~PartitionStorage() {
  // Note: shared_storage_ is owned by PartitionManager, not deleted here
}

CedarGraphStorage* PartitionStorage::GetEffectiveStorage() const {
  if (backend_) {
    return backend_->GetStorageForPartition(partition_id_);
  }
  return shared_storage_;
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
  CedarGraphStorage* storage = GetEffectiveStorage();
  if (!storage) {
    return Status::IOError("Storage not initialized");
  }
  
  if (is_readonly_.load()) {
    return Status::InvalidArgument("Partition is read-only");
  }
  
  // Inject partition_id into key
  CedarKey storage_key = InjectPartitionId(key);
  
  // Use CedarGraphStorage API: Put(entity_id, tx_time, descriptor, txn_version)
  Status s = storage->Put(
      storage_key.entity_id(),
      storage_key.timestamp().value(),
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
  CedarGraphStorage* storage = GetEffectiveStorage();
  if (!storage) {
    return Status::IOError("Storage not initialized");
  }
  
  // Inject partition_id into key
  CedarKey storage_key = InjectPartitionId(key);
  
  // Use CedarGraphStorage API: Get(entity_id, entity_type, column_id, timestamp)
  auto result = storage->Get(
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
                                 Timestamp commit_ts,
                                 Timestamp read_timestamp) {
  CedarGraphStorage* storage = GetEffectiveStorage();
  if (!storage) {
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
  // after the transaction's read_timestamp (snapshot isolation).
  // If read_timestamp is 0, skip validation (backward compatibility).
  if (storage && read_timestamp.value() > 0) {
    auto* lsm_engine = storage->GetLsmEngine();
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
          // If a version was committed after our read_timestamp, it's a conflict
          if (entry.txn_version > read_timestamp) {
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
  CedarGraphStorage* storage = GetEffectiveStorage();
  if (!storage) {
    return Status::IOError("Storage not initialized");
  }
  
  if (is_readonly_.load()) {
    return Status::InvalidArgument("Partition is read-only");
  }
  
  std::unique_lock<std::shared_mutex> lock(txn_mutex_);
  
  auto it = prepared_txns_.find(txn_id);
  if (it == prepared_txns_.end()) {
    auto committed_it = committed_txns_.find(txn_id);
    if (committed_it != committed_txns_.end()) {
      return Status::OK();  // Already committed
    }
    return Status::NotFound("Transaction not found: " + std::to_string(txn_id));
  }
  
  auto& state = it->second;
  if (state.status != DistributedTxnState::kPrepared &&
      state.status != DistributedTxnState::kCommitting) {
    return Status::InvalidArgument("Transaction not in committable state: " + std::to_string(txn_id));
  }
  state.status = DistributedTxnState::kCommitting;
  
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

  std::vector<cedar::cdc::ChangeRecord> records;
  std::vector<std::pair<uint64_t, cedar::cdc::ChangeRecord>> intent_records;
  if (change_log_ != nullptr && !validated_writes.empty()) {
    records.reserve(validated_writes.size());
    intent_records.reserve(validated_writes.size());
    for (const auto& entry : validated_writes) {
      cedar::cdc::ChangeRecord record;
      record.set_txn_id(txn_id);
      record.set_entity_id(entry.key.entity_id());
      record.set_target_id(entry.key.target_id());
      record.set_entity_type(static_cast<uint32_t>(entry.key.entity_type()));
      record.set_edge_type(entry.key.IsEdge() ? entry.key.column_id() : 0);
      record.set_column_id(entry.key.column_id());
      record.set_operation(entry.desc.IsTombstone()
                               ? cedar::cdc::CHANGE_OPERATION_DELETE
                               : cedar::cdc::CHANGE_OPERATION_UPDATE);
      record.set_valid_from(commit_ts.value());
      uint64_t raw_descriptor = entry.desc.AsRaw();
      record.set_payload(
          std::string(reinterpret_cast<const char*>(&raw_descriptor),
                      sizeof(raw_descriptor)));
      intent_records.push_back({entry.key.timestamp().value(), record});
      records.push_back(std::move(record));
    }
    CEDAR_RETURN_IF_ERROR(
        PersistCdcIntent(txn_id, commit_ts.value(), intent_records));
  }

  size_t written_count = 0;
  for (const auto& entry : validated_writes) {
    CedarKey storage_key = InjectPartitionId(entry.key);
    Status s = storage->Put(
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

  if (change_log_ != nullptr && !records.empty()) {
    Status s = change_log_->AppendCommittedBatch(commit_ts.value(),
                                                 std::move(records));
    if (!s.ok()) {
      state.status = DistributedTxnState::kCommitting;
      return Status::IOError(
          "CDC append failed during commit — recovery must retry commit: " +
          s.ToString());
    }
    Status delete_intent = DeleteCdcIntent(txn_id);
    if (!delete_intent.ok()) {
      std::cerr << "[PartitionStorage::Commit] CDC intent cleanup warning txn_id="
                << txn_id << " partition=" << partition_id_ << " error="
                << delete_intent.ToString() << std::endl;
    }
  }

  state.status = DistributedTxnState::kCommitted;
  WriteTxnWAL(txn_id, "COMMIT");
  committed_txns_.insert(txn_id);
  prepared_txns_.erase(it);
  
  return Status::OK();
}

std::string PartitionStorage::CdcIntentDir() const {
  const std::string root = manager_ ? manager_->GetDataRoot() : "/tmp/cedar_storage";
  return root + "/cdc_intents/partition_" + std::to_string(partition_id_);
}

std::string PartitionStorage::CdcIntentPath(TxnID txn_id) const {
  return CdcIntentDir() + "/txn_" + std::to_string(txn_id) + ".intent";
}

Status PartitionStorage::PersistCdcIntent(
    TxnID txn_id, uint64_t commit_version,
    const std::vector<std::pair<uint64_t, cedar::cdc::ChangeRecord>>& records) const {
  if (records.empty()) {
    return Status::OK();
  }
  fs::path dir(CdcIntentDir());
  std::error_code ec;
  fs::create_directories(dir, ec);
  if (ec) {
    return FilesystemStatus("create CDC intent directory", dir, ec);
  }

  fs::path final_path(CdcIntentPath(txn_id));
  fs::path tmp_path = final_path;
  tmp_path += ".tmp";

  int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    return Status::IOError("PersistCdcIntent", "failed to open temp intent");
  }

  auto write_all = [&](const void* data, size_t size) -> Status {
    const char* cursor = static_cast<const char*>(data);
    while (size > 0) {
      ssize_t written = ::write(fd, cursor, size);
      if (written <= 0) {
        return Status::IOError("PersistCdcIntent", "failed to write intent");
      }
      cursor += written;
      size -= static_cast<size_t>(written);
    }
    return Status::OK();
  };

  const std::string magic = kCdcIntentMagic;
  uint32_t magic_size = static_cast<uint32_t>(magic.size());
  uint64_t tid = txn_id;
  uint64_t version = commit_version;
  uint32_t record_count = static_cast<uint32_t>(records.size());
  Status s = write_all(&magic_size, sizeof(magic_size));
  if (s.ok()) s = write_all(magic.data(), magic.size());
  if (s.ok()) s = write_all(&tid, sizeof(tid));
  if (s.ok()) s = write_all(&version, sizeof(version));
  if (s.ok()) s = write_all(&record_count, sizeof(record_count));
  for (const auto& [storage_timestamp, record] : records) {
    if (!s.ok()) break;
    std::string serialized;
    if (!record.SerializeToString(&serialized)) {
      s = Status::Corruption("PersistCdcIntent", "failed to serialize record");
      break;
    }
    uint32_t record_size = static_cast<uint32_t>(serialized.size());
    s = write_all(&storage_timestamp, sizeof(storage_timestamp));
    if (!s.ok()) break;
    s = write_all(&record_size, sizeof(record_size));
    if (s.ok()) s = write_all(serialized.data(), serialized.size());
  }

  if (s.ok()) {
#ifdef __APPLE__
    if (::fcntl(fd, F_FULLFSYNC) < 0) {
      s = Status::IOError("PersistCdcIntent", "F_FULLFSYNC failed");
    }
#else
    if (::fsync(fd) < 0) {
      s = Status::IOError("PersistCdcIntent", "fsync failed");
    }
#endif
  }
  ::close(fd);
  if (!s.ok()) {
    fs::remove(tmp_path, ec);
    return s;
  }

  fs::rename(tmp_path, final_path, ec);
  if (ec) {
    return FilesystemStatus("rename CDC intent", tmp_path, ec);
  }

  int dir_fd = ::open(dir.c_str(), O_RDONLY);
  if (dir_fd >= 0) {
    ::fsync(dir_fd);
    ::close(dir_fd);
  }
  return Status::OK();
}

Status PartitionStorage::DeleteCdcIntent(TxnID txn_id) const {
  std::error_code ec;
  fs::remove(CdcIntentPath(txn_id), ec);
  if (ec) {
    return FilesystemStatus("remove CDC intent", CdcIntentPath(txn_id), ec);
  }
  return Status::OK();
}

Status PartitionStorage::RecoverCdcIntents() {
  if (change_log_ == nullptr) {
    return Status::OK();
  }
  fs::path dir(CdcIntentDir());
  std::error_code ec;
  if (!fs::exists(dir, ec)) {
    return Status::OK();
  }
  if (ec) {
    return FilesystemStatus("stat CDC intent directory", dir, ec);
  }

  for (const auto& entry : fs::directory_iterator(dir, ec)) {
    if (ec) {
      return FilesystemStatus("iterate CDC intent directory", dir, ec);
    }
    if (!entry.is_regular_file() || entry.path().extension() != ".intent") {
      continue;
    }
    auto batch_or = ReadCdcIntentFile(entry.path());
    if (!batch_or.ok()) {
      return batch_or.status();
    }
    auto batch = std::move(batch_or.ValueOrDie());
    bool already_published = false;
    uint64_t offset = 0;
    std::vector<bool> matched_indexes(batch.records.size(), false);
    while (true) {
      auto page = change_log_->ReadAfter(offset, 4096,
                                         std::numeric_limits<size_t>::max());
      if (!page.ok()) {
        return page.status();
      }
      const auto& records = page.ValueOrDie();
      if (records.empty()) {
        break;
      }
      for (const auto& record : records) {
        offset = std::max(offset, record.offset());
        if (record.txn_id() == batch.txn_id &&
            record.commit_version() == batch.commit_version &&
            record.batch_size() == batch.records.size() &&
            record.batch_index() < batch.records.size()) {
          const auto& expected = batch.records[record.batch_index()].second;
          if (record.entity_id() == expected.entity_id() &&
              record.target_id() == expected.target_id() &&
              record.entity_type() == expected.entity_type() &&
              record.column_id() == expected.column_id() &&
              record.operation() == expected.operation() &&
              record.payload() == expected.payload()) {
            matched_indexes[record.batch_index()] = true;
          }
        }
      }
      if (!matched_indexes.empty() &&
          std::all_of(matched_indexes.begin(), matched_indexes.end(),
                      [](bool matched) { return matched; })) {
        already_published = true;
        break;
      }
    }
    if (!already_published && !batch.records.empty()) {
      CedarGraphStorage* storage = GetEffectiveStorage();
      if (!storage) {
        return Status::IOError("RecoverCdcIntents", "Storage not initialized");
      }
      std::vector<cedar::cdc::ChangeRecord> records;
      records.reserve(batch.records.size());
      for (const auto& [storage_timestamp, record] : batch.records) {
        Descriptor desc;
        if (record.payload().size() >= sizeof(uint64_t)) {
          uint64_t raw_descriptor = 0;
          std::memcpy(&raw_descriptor, record.payload().data(),
                      sizeof(raw_descriptor));
          desc = Descriptor(raw_descriptor);
        }
        CEDAR_RETURN_IF_ERROR(storage->Put(record.entity_id(),
                                           storage_timestamp, desc,
                                           Timestamp(batch.commit_version)));
        records.push_back(record);
      }
      CEDAR_RETURN_IF_ERROR(change_log_->AppendCommittedBatch(
          batch.commit_version, std::move(records)));
    }
    std::error_code remove_ec;
    fs::remove(entry.path(), remove_ec);
    if (remove_ec) {
      return FilesystemStatus("remove recovered CDC intent", entry.path(),
                              remove_ec);
    }
  }
  return Status::OK();
}

Status PartitionStorage::Abort(TxnID txn_id) {
  std::unique_lock<std::shared_mutex> lock(txn_mutex_);
  
  auto it = prepared_txns_.find(txn_id);
  if (it == prepared_txns_.end()) {
    auto aborted_it = aborted_txns_.find(txn_id);
    if (aborted_it != aborted_txns_.end()) {
      return Status::OK();  // Already aborted
    }
    return Status::NotFound("Transaction not found: " + std::to_string(txn_id));
  }
  
  it->second.status = DistributedTxnState::kAborted;
  
  // Write WAL
  WriteTxnWAL(txn_id, "ABORT");
  
  aborted_txns_.insert(txn_id);
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
  
  // Estimate partition stats from storage
  CedarGraphStorage* storage = GetEffectiveStorage();
  if (storage) {
    // Get storage statistics
    auto storage_stats = storage->GetStats();
    
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
  const fs::path snapshot_dir(snapshot_path);
  std::error_code ec;
  fs::create_directories(snapshot_dir, ec);
  if (ec) {
    return FilesystemStatus("SaveSnapshotForMigration create snapshot directory",
                            snapshot_dir, ec);
  }

  const fs::path data_root(GetDataRoot());
  auto canonical_data_root = fs::weakly_canonical(data_root, ec);
  if (ec) {
    return FilesystemStatus("SaveSnapshotForMigration resolve data root",
                            data_root, ec);
  }
  auto canonical_snapshot_dir = fs::weakly_canonical(snapshot_dir, ec);
  if (ec) {
    return FilesystemStatus("SaveSnapshotForMigration resolve snapshot directory",
                            snapshot_dir, ec);
  }
  if (canonical_data_root == canonical_snapshot_dir) {
    return Status::InvalidArgument(
        "Snapshot path cannot be the partition data root",
        canonical_snapshot_dir.string());
  }

  fs::recursive_directory_iterator it(
      canonical_data_root, fs::directory_options::skip_permission_denied, ec);
  if (ec) {
    return FilesystemStatus("SaveSnapshotForMigration iterate data root",
                            canonical_data_root, ec);
  }
  const fs::recursive_directory_iterator end;
  for (; it != end; it.increment(ec)) {
    if (ec) {
      return FilesystemStatus("SaveSnapshotForMigration advance iterator",
                              canonical_data_root, ec);
    }

    const auto& entry = *it;
    auto canonical_entry = fs::weakly_canonical(entry.path(), ec);
    if (ec) {
      return FilesystemStatus("SaveSnapshotForMigration resolve entry",
                              entry.path(), ec);
    }

    if (entry.is_directory(ec)) {
      if (IsSameOrChildPath(canonical_entry, canonical_snapshot_dir) ||
          IsMigrationSnapshotDirectory(canonical_entry)) {
        it.disable_recursion_pending();
      }
      continue;
    }
    if (ec) {
      return FilesystemStatus("SaveSnapshotForMigration inspect directory",
                              entry.path(), ec);
    }

    if (IsSameOrChildPath(canonical_entry, canonical_snapshot_dir)) {
      continue;
    }
    if (!entry.is_regular_file(ec)) {
      if (ec) {
        return FilesystemStatus("SaveSnapshotForMigration inspect file",
                                entry.path(), ec);
      }
      continue;
    }

    auto rel_path = fs::relative(canonical_entry, canonical_data_root, ec);
    if (ec) {
      return FilesystemStatus("SaveSnapshotForMigration derive relative path",
                              canonical_entry, ec);
    }
    if (rel_path.is_absolute() || HasParentTraversal(rel_path)) {
      return Status::InvalidArgument("Unsafe snapshot relative path",
                                     rel_path.string());
    }

    auto dst_path = canonical_snapshot_dir / rel_path;
    fs::create_directories(dst_path.parent_path(), ec);
    if (ec) {
      return FilesystemStatus("SaveSnapshotForMigration create destination directory",
                              dst_path.parent_path(), ec);
    }
    fs::copy_file(canonical_entry, dst_path,
                  fs::copy_options::overwrite_existing, ec);
    if (ec) {
      return FilesystemStatus("SaveSnapshotForMigration copy file", dst_path, ec);
    }
  }

  auto txn_state_path = (canonical_snapshot_dir / "txn_state").string();
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
