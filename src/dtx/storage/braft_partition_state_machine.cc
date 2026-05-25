// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include "cedar/dtx/storage/braft_partition_state_machine.h"
#include "cedar/dtx/storage_impl/partition_storage.h"

#include <butil/logging.h>
#include <fstream>
#include <filesystem>

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// StorageRaftCommand Serialization
// =============================================================================
// Format: [type:1][key:32][descriptor_raw:8][txn_version:8] = 49 bytes

std::string StorageRaftCommand::Serialize() const {
  std::string result;
  result.reserve(49);
  
  uint8_t type_val = static_cast<uint8_t>(type);
  result.push_back(static_cast<char>(type_val));
  
  std::string key_data = key.Encode();
  result.append(key_data);  // 32 bytes
  
  uint64_t desc_raw = descriptor.AsRaw();
  result.append(reinterpret_cast<const char*>(&desc_raw), sizeof(desc_raw));
  
  uint64_t ts = static_cast<uint64_t>(txn_version);
  result.append(reinterpret_cast<const char*>(&ts), sizeof(ts));
  
  return result;
}

StatusOr<StorageRaftCommand> StorageRaftCommand::Deserialize(const std::string& data) {
  if (data.size() < 49) {
    return Status::InvalidArgument("StorageRaftCommand", "data too short");
  }
  
  StorageRaftCommand cmd;
  size_t pos = 0;
  
  cmd.type = static_cast<Type>(static_cast<uint8_t>(data[pos]));
  pos += 1;
  
  auto key_opt = CedarKey::Decode(std::string_view(data.data() + pos, CedarKey::kKeySize));
  if (!key_opt) {
    return Status::InvalidArgument("StorageRaftCommand", "invalid key");
  }
  cmd.key = *key_opt;
  pos += CedarKey::kKeySize;
  
  uint64_t desc_raw;
  std::memcpy(&desc_raw, data.data() + pos, sizeof(desc_raw));
  cmd.descriptor = Descriptor(desc_raw);
  pos += sizeof(desc_raw);
  
  uint64_t ts;
  std::memcpy(&ts, data.data() + pos, sizeof(ts));
  cmd.txn_version = Timestamp(ts);
  
  return cmd;
}

// =============================================================================
// PartitionRaftStateMachine Implementation
// =============================================================================

PartitionRaftStateMachine::PartitionRaftStateMachine(PartitionStorage* storage)
    : storage_(storage) {}

void PartitionRaftStateMachine::on_apply(braft::Iterator& iter) {
  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard closure_guard(iter.done());
    
    butil::IOBuf data_buf = iter.data();
    std::string data = data_buf.to_string();
    
    auto cmd_result = StorageRaftCommand::Deserialize(data);
    if (!cmd_result.ok()) {
      LOG(ERROR) << "Failed to deserialize storage command at index=" << iter.index()
                 << ": " << cmd_result.status().ToString();
      iter.set_error_and_rollback();
      return;
    }

    const auto& cmd = cmd_result.value();

    if (!storage_) {
      LOG(ERROR) << "No storage available for apply at index=" << iter.index();
      iter.set_error_and_rollback();
      return;
    }
    
    if (cmd.type == StorageRaftCommand::Type::kPut) {
      auto status = storage_->Put(cmd.key, cmd.descriptor, cmd.txn_version, 0);
      if (!status.ok()) {
        LOG(ERROR) << "Apply PUT failed at index=" << iter.index()
                   << ": " << status.ToString();
      }
    } else if (cmd.type == StorageRaftCommand::Type::kDelete) {
      auto status = storage_->Put(cmd.key, Descriptor(), cmd.txn_version, 0);
      if (!status.ok()) {
        LOG(ERROR) << "Apply DELETE failed at index=" << iter.index()
                   << ": " << status.ToString();
      }
    }
    
    last_applied_index_.store(iter.index());
  }
}

// Helper: Recursively copy all files from src_dir to dst_dir
// Returns a list of relative file paths
static std::vector<std::string> CopyDirectoryContents(
    const std::string& src_dir,
    const std::string& dst_dir) {
  std::vector<std::string> copied_files;
  if (!std::filesystem::exists(src_dir)) {
    return copied_files;
  }
  std::filesystem::create_directories(dst_dir);
  for (const auto& entry : std::filesystem::recursive_directory_iterator(src_dir)) {
    if (entry.is_regular_file()) {
      std::string relative_path = std::filesystem::relative(entry.path(), src_dir).string();
      std::string dst_path = dst_dir + "/" + relative_path;
      std::filesystem::create_directories(std::filesystem::path(dst_path).parent_path());
      try {
        std::filesystem::copy_file(entry.path(), dst_path,
                                   std::filesystem::copy_options::overwrite_existing);
        copied_files.push_back(relative_path);
      } catch (const std::filesystem::filesystem_error& e) {
        LOG(WARNING) << "Failed to copy file " << entry.path().string()
                     << " to " << dst_path << ": " << e.what();
      }
    }
  }
  return copied_files;
}

void PartitionRaftStateMachine::on_snapshot_save(braft::SnapshotWriter* writer,
                                                  braft::Closure* done) {
  braft::AsyncClosureGuard done_guard(done);
  
  if (!storage_) {
    LOG(WARNING) << "Storage not available for snapshot save";
    return;
  }
  
  // Step 1: Flush underlying storage to ensure all data is on disk
  // (Similar to NebulaGraph's write-blocking + checkpoint creation)
  auto* shared_storage = storage_->GetSharedStorage();
  if (shared_storage) {
    auto flush_status = shared_storage->ForceFlush();
    if (!flush_status.ok()) {
      LOG(WARNING) << "ForceFlush failed during snapshot: " << flush_status.ToString();
    }
  }
  
  // Step 2: Copy data files from storage data_root to snapshot directory
  // NebulaGraph uses RocksDB Checkpoint (hard links). We use file copy
  // since LsmEngine doesn't support checkpoint API.
  std::string data_root = storage_->GetDataRoot();
  std::string snapshot_path = writer->get_path();
  auto copied_files = CopyDirectoryContents(data_root, snapshot_path + "/data");
  
  // Register copied data files with snapshot writer
  for (const auto& rel_path : copied_files) {
    std::string snapshot_file = "data/" + rel_path;
    if (writer->add_file(snapshot_file, nullptr) != 0) {
      LOG(ERROR) << "Failed to add file to snapshot: " << snapshot_file;
      if (done) {
        done->status().set_error(EIO, "Failed to add snapshot file");
      }
      return;
    }
  }
  
  // Step 3: Serialize prepared transaction state (2PC)
  std::string txn_state_path = snapshot_path + "/txn_state";
  auto status = storage_->SavePreparedTxns(txn_state_path);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to save prepared txns for snapshot: " << status.ToString();
    if (done) {
      done->status().set_error(EIO, "Failed to save txn state");
    }
    return;
  }
  if (writer->add_file("txn_state", nullptr) != 0) {
    LOG(ERROR) << "Failed to add txn_state to snapshot";
    if (done) {
      done->status().set_error(EIO, "Failed to add txn_state");
    }
    return;
  }
  
  LOG(INFO) << "Snapshot saved for partition " << storage_->GetPartitionId()
            << " with " << copied_files.size() << " data files";
}

int PartitionRaftStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
  if (!storage_) {
    LOG(WARNING) << "Storage not available for snapshot load";
    return 0;
  }
  
  std::string snapshot_path = reader->get_path();
  std::string data_root = storage_->GetDataRoot();
  
  // Step 1: Restore data files from snapshot to storage data_root
  std::string snapshot_data_dir = snapshot_path + "/data";
  if (std::filesystem::exists(snapshot_data_dir)) {
    auto copied_files = CopyDirectoryContents(snapshot_data_dir, data_root);
    LOG(INFO) << "Restored " << copied_files.size() << " data files from snapshot"
              << " for partition " << storage_->GetPartitionId();
  }
  
  // Step 2: Restore prepared transaction state (2PC)
  std::string txn_state_path = snapshot_path + "/txn_state";
  if (std::filesystem::exists(txn_state_path)) {
    auto status = storage_->LoadPreparedTxns(txn_state_path);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to load prepared txns from snapshot: " << status.ToString();
      return -1;
    }
    LOG(INFO) << "Loaded prepared txns from snapshot for partition "
              << storage_->GetPartitionId();
  }
  
  return 0;
}

void PartitionRaftStateMachine::on_leader_start(int64_t term) {
  LOG(INFO) << "Partition state machine became leader, term=" << term;
}

void PartitionRaftStateMachine::on_leader_stop(const butil::Status& status) {
  LOG(INFO) << "Partition state machine stopped being leader: " << status.error_str();
}

void PartitionRaftStateMachine::on_shutdown() {
  LOG(INFO) << "Partition state machine shutting down";
}

void PartitionRaftStateMachine::on_error(const braft::Error& e) {
  LOG(ERROR) << "Partition state machine error: " << e.status().error_str();
}

void PartitionRaftStateMachine::on_configuration_committed(const braft::Configuration& conf) {
  (void)conf;
}

void PartitionRaftStateMachine::on_stop_following(const braft::LeaderChangeContext& ctx) {
  (void)ctx;
}

void PartitionRaftStateMachine::on_start_following(const braft::LeaderChangeContext& ctx) {
  (void)ctx;
}

}  // namespace storage
}  // namespace dtx
}  // namespace cedar
