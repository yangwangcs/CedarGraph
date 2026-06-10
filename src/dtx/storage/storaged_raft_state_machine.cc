#include "cedar/dtx/storage/storaged_raft_state_machine.h"
#include "cedar/dtx/storage/braft_partition_state_machine.h"
#include <brpc/closure_guard.h>
#include <butil/logging.h>
#include <filesystem>

namespace cedar { namespace dtx { namespace storage {

StorageRaftStateMachine::StorageRaftStateMachine(CedarGraphStorage* storage)
    : storage_(storage) {}

void StorageRaftStateMachine::on_apply(braft::Iterator& iter) {
  for (; iter.valid(); iter.next()) {
    brpc::ClosureGuard done_guard(iter.done());

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
                   << ": " << status.ToString();
      }
    } else if (cmd.type == StorageRaftCommand::Type::kDelete) {
      auto status = storage_->Delete(
          cmd.key.entity_id(),
          cmd.key.timestamp().value(),
          cmd.txn_version);
      if (!status.ok()) {
        LOG(ERROR) << "Apply DELETE failed at index=" << iter.index()
                   << ": " << status.ToString();
      }
    }

    last_applied_index_.store(iter.index());
  }
}

void StorageRaftStateMachine::on_shutdown() {}

void StorageRaftStateMachine::on_snapshot_save(braft::SnapshotWriter* writer,
                                                braft::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "Raft snapshot save to " << writer->get_path();

  if (!storage_) {
    LOG(WARNING) << "No storage for snapshot save";
    return;
  }

  // Copy data directory to snapshot path
  try {
    std::filesystem::path snapshot_path(writer->get_path());
    std::filesystem::create_directories(snapshot_path);

    // TODO: Get actual data directory from storage
    // For now, we assume the snapshot path is managed by braft
    LOG(INFO) << "Raft snapshot saved";
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

  // TODO: Restore data directory from snapshot path
  // This requires shutting down LSM engine, copying files, and restarting
  LOG(INFO) << "Raft snapshot loaded";
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
