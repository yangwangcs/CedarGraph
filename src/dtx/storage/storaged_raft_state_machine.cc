#include "cedar/dtx/storage/storaged_raft_state_machine.h"
#include "cedar/dtx/storage/braft_partition_state_machine.h"
#include <braft/raft.h>
#include <butil/logging.h>
#include <filesystem>

namespace cedar { namespace dtx { namespace storage {

StorageRaftStateMachine::StorageRaftStateMachine(CedarGraphStorage* storage)
    : storage_(storage) {}

void StorageRaftStateMachine::on_apply(braft::Iterator& iter) {
  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard done_guard(iter.done());

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
      auto status = storage_->Put(
          cmd.key.entity_id(),
          cmd.key.timestamp().value(),
          cmd.descriptor,
          cmd.txn_version);
      if (!status.ok()) {
        LOG(ERROR) << "Apply PUT failed at index=" << iter.index()
                   << ": " << status.ToString()
                   << " — stepping down";
        iter.set_error_and_rollback();
        return;
      }
    } else if (cmd.type == StorageRaftCommand::Type::kDelete) {
      auto status = storage_->Delete(
          cmd.key.entity_id(),
          cmd.key.timestamp().value(),
          cmd.txn_version);
      if (!status.ok()) {
        LOG(ERROR) << "Apply DELETE failed at index=" << iter.index()
                   << ": " << status.ToString()
                   << " — stepping down";
        iter.set_error_and_rollback();
        return;
      }
    }

    last_applied_index_.store(iter.index());
  }
}

void StorageRaftStateMachine::on_shutdown() {}

void StorageRaftStateMachine::on_snapshot_save(braft::SnapshotWriter* writer,
                                                braft::Closure* done) {
  braft::AsyncClosureGuard done_guard(done);
  LOG(INFO) << "Raft snapshot save to " << writer->get_path();

  if (!storage_) {
    LOG(WARNING) << "No storage for snapshot save";
    return;
  }

  try {
    std::string snapshot_path = writer->get_path();
    std::string data_path = storage_->GetDbPath();

    // Step 1: Pause compaction to prevent SST file deletion during copy
    storage_->PauseCompaction();

    // Step 2: Flush underlying storage to ensure all data is on disk
    auto flush_status = storage_->ForceFlush();
    if (!flush_status.ok()) {
      LOG(WARNING) << "ForceFlush failed during snapshot: "
                   << flush_status.ToString();
    }

    // Step 3: Copy data directory to snapshot path
    std::string snapshot_data_dir = snapshot_path + "/data";
    std::filesystem::create_directories(snapshot_data_dir);

    if (std::filesystem::exists(data_path)) {
      for (const auto& entry :
           std::filesystem::recursive_directory_iterator(data_path)) {
        if (entry.is_regular_file()) {
          std::string relative =
              std::filesystem::relative(entry.path(), data_path).string();
          std::string dst = snapshot_data_dir + "/" + relative;
          std::filesystem::create_directories(
              std::filesystem::path(dst).parent_path());
          std::filesystem::copy_file(
              entry.path(), dst,
              std::filesystem::copy_options::overwrite_existing);
        }
      }
    }

    // Step 4: Resume compaction
    storage_->ResumeCompaction();

    // Register data files with snapshot writer
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(snapshot_data_dir)) {
      if (entry.is_regular_file()) {
        std::string relative =
            std::filesystem::relative(entry.path(), snapshot_data_dir)
                .string();
        std::string snapshot_file = "data/" + relative;
        if (writer->add_file(snapshot_file, nullptr) != 0) {
          LOG(ERROR) << "Failed to add file to snapshot: " << snapshot_file;
          if (done) {
            done->status().set_error(EIO, "Failed to add snapshot file");
          }
          return;
        }
      }
    }

    // Step 3: Serialize prepared transaction state (2PC)
    std::string txn_state_path = snapshot_path + "/txn_state";
    auto status = storage_->SavePreparedTxns(txn_state_path);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to save prepared txns for snapshot: "
                 << status.ToString();
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

    LOG(INFO) << "Raft snapshot saved to " << snapshot_path;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Snapshot save failed: " << e.what();
    if (done) {
      done->status().set_error(EIO, "Snapshot save failed: %s", e.what());
    }
  }
}

int StorageRaftStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
  LOG(INFO) << "Raft snapshot load from " << reader->get_path();

  if (!storage_) {
    LOG(WARNING) << "No storage for snapshot load";
    return 0;
  }

  std::string snapshot_path = reader->get_path();

  // Step 1: Restore data files from snapshot to storage data_root
  std::string snapshot_data_dir = snapshot_path + "/data";
  if (std::filesystem::exists(snapshot_data_dir)) {
    auto status = storage_->RestoreFromSnapshot(snapshot_data_dir);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to restore snapshot data: " << status.ToString();
      return -1;
    }
    LOG(INFO) << "Restored data files from snapshot";
  }

  // Step 2: Restore prepared transaction state (2PC)
  std::string txn_state_path = snapshot_path + "/txn_state";
  if (std::filesystem::exists(txn_state_path)) {
    auto status = storage_->LoadPreparedTxns(txn_state_path);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to load prepared txns from snapshot: "
                 << status.ToString();
      return -1;
    }
    LOG(INFO) << "Loaded prepared txns from snapshot";
  }

  return 0;
}

void StorageRaftStateMachine::on_leader_start(int64_t term) {
  LOG(INFO) << "Raft leader started, term=" << term;
}

void StorageRaftStateMachine::on_leader_stop(const butil::Status& status) {
  LOG(INFO) << "Raft leader stopped: " << status.error_str();
}

void StorageRaftStateMachine::on_error(const braft::Error& e) {
  LOG(ERROR) << "Raft error: " << e.status().error_str();
}

void StorageRaftStateMachine::on_configuration_committed(const braft::Configuration& conf) {
  LOG(INFO) << "Raft configuration committed";
}

void StorageRaftStateMachine::on_stop_following(const braft::LeaderChangeContext& ctx) {
  LOG(INFO) << "Raft stop following leader " << ctx.leader_id();
}

void StorageRaftStateMachine::on_start_following(const braft::LeaderChangeContext& ctx) {
  LOG(INFO) << "Raft start following leader " << ctx.leader_id();
}

}}}  // namespace cedar::dtx::storage
